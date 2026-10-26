use tokio_stream::{Stream, StreamExt};

use std::cell::RefCell;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::{Metrics, Position, Span};

/// Lazy string buffer that fills up on demand, can be iterated and indexed by
/// character position.
///
/// The [`AsyncSourceBuffer`] wraps aroung a `char` stream. It can be itself used as
/// a `char` stream, or as a `SourceBuffer` to access an arbitrary fragment of
/// the input source stream.
pub struct AsyncSourceBuffer<E, I: Stream<Item = Result<char, E>>, M: Metrics> {
	p: RefCell<Inner<E, I>>,

	/// Metrics used.
	metrics: M,
}

struct Inner<E, I: Stream<Item = Result<char, E>>> {
	/// Input source `char` stream.
	input: Pin<Box<I>>,

	/// SourceBuffer error state.
	error: Option<E>,

	/// Buffer data.
	data: Vec<char>,

	/// Lines index.
	///
	/// Contains the index of the first character of each line.
	lines: Vec<usize>,

	/// Span of the buffer.
	span: Span,
}

impl<E, I: Stream<Item = Result<char, E>>> Inner<E, I> {
	/// Read the next line from the input stream and add it to the buffer.
	/// Returns `true` if a new line has been added. Returns `false` if the
	/// source stream is done.
	async fn read_line<M: Metrics>(&mut self, metrics: &M) -> bool {
		if self.error.is_none() {
			let line = self.span.end().line;
			while line == self.span.end().line {
				match self.input.next().await {
					Some(Ok(c)) => {
						self.data.push(c);
						self.span.push(c, metrics);
					}
					Some(Err(e)) => {
						self.error = Some(e);
						return false;
					}
					None => return false,
				}
			}

			// register the next line index.
			self.lines.push(self.data.len());

			true
		} else {
			false
		}
	}

	/// Get the index of the char at the given cursor position if it is in the
	/// buffer. If it is not in the buffer but after the buffered content,
	/// the input stream will be read until the buffer span includes the
	/// given position.
	///
	/// Returns `None` if the given position if previous to the buffer start
	/// positions, if the source stream ends before the given position, or
	/// if the line at the given position is shorter than the given position
	/// column.
	async fn index_at<M: Metrics>(&mut self, pos: Position, metrics: &M) -> Result<Option<usize>, E> {
		if pos < self.span.start() {
			Ok(None)
		} else {
			while pos >= self.span.end() && self.read_line(metrics).await {}

			if pos >= self.span.end() {
				let mut error = None;
				std::mem::swap(&mut error, &mut self.error);
				match error {
					Some(e) => Err(e),
					None => Ok(None),
				}
			} else {
				// line index relative to the first line of the buffer.
				let relative_line = pos.line - self.span.start().line;
				// get the index of the char of the begining of the line in the buffer.
				let mut i = self.lines[relative_line];
				// place a virtual cursor at the begining of the target line.
				let mut cursor = Position::new(pos.line, 0);

				while cursor < pos {
					cursor = cursor.next(self.data[i], metrics);
					i += 1;
				}

				if cursor == pos {
					// found it!
					Ok(Some(i))
				} else {
					// the position does not exist in the buffer.
					Ok(None)
				}
			}
		}
	}

	/// Get the character at the given index.
	///
	/// If it is not in the buffer but after the buffered content, the input
	/// stream will be read until the buffer span includes the given
	/// position. Returns `None` if the source stream ends before the given
	/// position.
	async fn get<M: Metrics>(&mut self, i: usize, metrics: &M) -> Result<Option<char>, E> {
		while i >= self.data.len() && self.read_line(metrics).await {}

		if i >= self.data.len() {
			let mut error = None;
			std::mem::swap(&mut error, &mut self.error);
			match error {
				Some(e) => Err(e),
				None => Ok(None),
			}
		} else {
			Ok(Some(self.data[i]))
		}
	}
}

impl<E, I: Stream<Item = Result<char, E>>, M: Metrics> AsyncSourceBuffer<E, I, M> {
	/// Create a new empty buffer starting at the given position.
	pub fn new(input: I, position: Position, metrics: M) -> Self {
		Self {
			p: RefCell::new(Inner {
				input: Box::pin(input),
				error: None,
				data: Vec::new(),
				lines: vec![0],
				span: position.into(),
			}),
			metrics,
		}
	}

	/// Get the metrics used by the source buffer to map every character.
	pub fn metrics(&self) -> &M { &self.metrics }

	/// Get the span of the entire buffered data.
	pub fn span(&self) -> Span { self.p.borrow().span }

	/// Get the index of the char at the given cursor position if it is in the
	/// buffer. If it is not in the buffer but after the buffered content,
	/// the input stream will be read until the buffer span includes the
	/// given position.
	///
	/// Returns `None` if the given position if previous to the buffer start
	/// positions, if the source stream ends before the given position, or
	/// if the line at the given position is shorter than the given position
	/// column.
	pub async fn index_at(&self, pos: Position) -> Result<Option<usize>, E> {
		self.p.borrow_mut().index_at(pos, &self.metrics).await
	}

	/// Get the char at the given position if it is in the buffer.
	/// If it is not in the buffer yet, the input stream will be pulled until
	/// the buffer span includes the given position.
	///
	/// Returns `None` if the given position is out of range, if the source
	/// stream ends before the given position, or if the line at the given
	/// position is shorter than the given position column.
	pub async fn at(&self, pos: Position) -> Result<Option<char>, E> {
		match self.index_at(pos).await {
			Ok(Some(i)) => self.p.borrow_mut().get(i, &self.metrics).await,
			Ok(None) => Ok(None),
			Err(e) => Err(e)
		}
	}

	/// Get the character at the given index.
	///
	/// If it is not in the buffer but after the buffered content, the input
	/// stream will be read until the buffer span includes the given
	/// position. Returns `None` if the source stream ends before the given
	/// position.
	pub async fn get(&self, i: usize) -> Result<Option<char>, E> { self.p.borrow_mut().get(i, &self.metrics).await }

	/// Returns an iterator through the characters of the buffer from the
	/// begining of it.
	///
	/// When it reaches the end of the buffer, the buffer will start reading
	/// from the source stream.
	pub fn iter(&self) -> Iter<'_, E, I, M> {
		Iter {
			buffer: self,
			i: Some(Ok(0)),
			pos: self.p.borrow().span.start(),
			end: Position::end(),
            future: None,
		}
	}

	/// Returns an iterator through the characters of the buffer from the given
	/// position.
	///
	/// If the input position precedes the buffer start position, then it will
	/// start from the buffer start position.
	/// When it reaches the end of the buffer, the buffer will start reading
	/// from the source stream.
	pub async fn iter_from(&self, pos: Position) -> Iter<'_, E, I, M> {
		let start = self.p.borrow().span.start();
		let pos = std::cmp::max(start, pos);

		Iter {
			buffer: self,
			i: self.index_at(pos).await.transpose(),
			pos,
			end: Position::end(),
			future: None,
		}
	}

	/// Returns an iterator through the characters of the buffer in the given
	/// span.
	///
	/// If the input start position precedes the buffer start position, then it
	/// will start from the buffer start position.
	/// When it reaches the end of the buffer, the buffer will start reading
	/// from the source stream.
	pub async fn iter_span(&self, span: Span) -> Iter<'_, E, I, M> {
		let start = self.p.borrow().span.start();
		let pos = std::cmp::max(start, span.start());

		Iter {
			buffer: self,
			i: self.index_at(pos).await.transpose(),
			pos,
			end: span.end(),
			future: None,
		}
	}
}

type GetFuture<'b, E> = Pin<Box<dyn std::future::Future<Output = Result<Option<char>, E>> + 'b>>;

/// Iterator over the characters of a [`SourceBuffer`].
///
/// This iterator is created using the [`SourceBuffer::iter`] method or the
/// [`SourceBuffer::iter_from`] method. When it reaches the end of the buffer,
/// the buffer will start reading from the source stream until the stream itself
/// return `None`.
pub struct Iter<'b, E, I: 'b + Stream<Item = Result<char, E>>, M: Metrics> {
	buffer: &'b AsyncSourceBuffer<E, I, M>,
	i: Option<Result<usize, E>>,
	pos: Position,
	end: Position,
    future: Option<GetFuture<'b, E>>,
}

impl<'b, E: std::fmt::Debug + Unpin, I: 'b + Stream<Item = Result<char, E>>, M: Metrics> Iter<'b, E, I, M> {
	pub async fn into_string(mut self) -> Result<String, E> {
		let mut string = String::new();

		while let Some(c) = self.next().await {
			string.push(c?);
		}

		Ok(string)
	}
}

impl<'b, E: std::fmt::Debug, I: 'b + Stream<Item = Result<char, E>>, M: Metrics> Stream for Iter<'b, E, I, M> {
    type Item = Result<char, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // SAFETY: the pinned future remains pinned inside its Box;
        // moving the Pin<Box<_>> handle does not move the future.
        let this = unsafe { self.get_unchecked_mut() };

        if this.pos >= this.end {
            return Poll::Ready(None);
        }

        match &this.i {
            Some(Ok(i)) => {
                if this.future.is_none() {
                    this.future = Some(Box::pin(this.buffer.get(*i)));
                }

                match this.future.as_mut().unwrap().as_mut().poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(Ok(Some(c))) => {
                        this.future = None;
                        this.pos = this.pos.next(c, this.buffer.metrics());
                        *this.i.as_mut().unwrap().as_mut().unwrap() += 1;
                        Poll::Ready(Some(Ok(c)))
                    }
                    Poll::Ready(Ok(None)) => {
                        this.future = None;
                        Poll::Ready(None)
                    }
                    Poll::Ready(Err(e)) => {
                        this.future = None;
                        Poll::Ready(Some(Err(e)))
                    }
                }
            }
            None => Poll::Ready(None),
            Some(Err(_)) => {
                let Some(Err(e)) = this.i.take() else {
                    unreachable!()
                };
                Poll::Ready(Some(Err(e)))
            }
        }
    }
}
