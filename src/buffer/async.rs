use tokio_stream::{Stream, StreamExt};

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
	state: tokio::sync::RwLock<Inner<E>>,

	input: tokio::sync::Mutex<Pin<Box<I>>>,

	/// Metrics used.
	metrics: M,
}

/// The tracked state of the input stream.
struct Inner<E> {
	/// SourceBuffer error state.
	error: Option<E>,

	/// Whether EOF has been reached.
	done: bool,

	/// Buffer data.
	data: Vec<char>,

	/// Lines index.
	///
	/// Contains the index of the first character of each line.
	lines: Vec<usize>,

	/// Span of the buffer.
	span: Span,
}

impl<E, I: Stream<Item = Result<char, E>>, M: Metrics> AsyncSourceBuffer<E, I, M> {
	/// Create a new empty buffer starting at the given position.
	pub fn new(input: I, position: Position, metrics: M) -> Self {
		Self {
			state: tokio::sync::RwLock::const_new(Inner {
				error: None,
				done: false,
				data: Vec::new(),
				lines: vec![0],
				span: position.into(),
			}),
			input: tokio::sync::Mutex::new(Box::pin(input)),
			metrics,
		}
	}

	/// Read the next line from the input stream and add it to the buffer.
	/// Returns `true` if a new line has been added or was not needed. Returns
	/// `false` if the source stream is done.
	async fn read_line(&self, pos: Option<Position>) -> bool {
		let mut input = self.input.lock().await;

		// in case a read was performed while we were waiting on the input lock
		let line = {
			let state = self.state.read().await;
			if state.done || state.error.is_some() {
				return false;
			} else if pos.is_some_and(|p| p < state.span.end()) {
				return true;
			}
			state.span.end().line
		};

		loop {
			let item = input.next().await;

			let mut state = self.state.write().await;

			match item {
				Some(Ok(c)) => {
					state.data.push(c);
					state.span.push(c, &self.metrics);

					if state.span.end().line != line {
						let len = state.data.len();
						state.lines.push(len);
						break true;
					}
				},
				Some(Err(e)) => {
					state.error = Some(e);
					break false;
				},
				None => {
					state.done = true;
					break false
				},
			}
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
	pub async fn index_at(&self, pos: Position) -> Result<Option<usize>, E> {
		if pos < self.state.read().await.span.start() {
			Ok(None)
		} else {
			while self.read_line(Some(pos)).await && pos >= self.state.read().await.span.end() {}
			let span = self.state.read().await.span;

			if pos >= span.end() {
				let mut state = self.state.write().await;
				let mut error = None;
				std::mem::swap(&mut error, &mut state.error);
				match error {
					Some(e) => Err(e),
					None => Ok(None),
				}
			} else {
				let state = self.state.read().await;
				// line index relative to the first line of the buffer.
				let relative_line = pos.line - span.start().line;
				// get the index of the char of the begining of the line in the buffer.
				let mut i = state.lines[relative_line];
				// place a virtual cursor at the begining of the target line.
				let mut cursor = Position::new(pos.line, 0);

				while cursor < pos {
					cursor = cursor.next(state.data[i], &self.metrics);
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
	pub async fn get(&self, i: usize) -> Result<Option<char>, E> {
		while self.read_line(None).await && i >= self.state.read().await.data.len() {}
		let len = self.state.read().await.data.len();

		if i >= len {
			let mut state = self.state.write().await;
			let mut error = None;
			std::mem::swap(&mut error, &mut state.error);
			match error {
				Some(e) => Err(e),
				None => Ok(None),
			}
		} else {
			let state = self.state.read().await;
			Ok(Some(state.data[i]))
		}
	}

	/// Get the metrics used by the source buffer to map every character.
	pub fn metrics(&self) -> &M { &self.metrics }

	/// Get the span of the entire buffered data.
	pub fn span(&self) -> Span { self.state.blocking_read().span }

	/// Get the char at the given position if it is in the buffer.
	/// If it is not in the buffer yet, the input stream will be pulled until
	/// the buffer span includes the given position.
	///
	/// Returns `None` if the given position is out of range, if the source
	/// stream ends before the given position, or if the line at the given
	/// position is shorter than the given position column.
	pub async fn at(&self, pos: Position) -> Result<Option<char>, E> {
		match self.index_at(pos).await {
			Ok(Some(i)) => self.get(i).await,
			Ok(None) => Ok(None),
			Err(e) => Err(e)
		}
	}

	/// Returns a stream through the characters of the buffer from the
	/// beginning of it.
	///
	/// When it reaches the end of the buffer, the buffer will start reading
	/// from the source stream.
	pub async fn iter(&self) -> Iter<'_, E, I, M> {
		Iter {
			buffer: self,
			i: Some(Ok(0)),
			pos: self.state.read().await.span.start(),
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
		let start = self.state.read().await.span.start();
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
		let start = self.state.read().await.span.start();
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

impl<'b, E, I, M> std::marker::Unpin for Iter<'b, E, I, M>
where
	E: std::fmt::Debug,
	I: 'b + Stream<Item = Result<char, E>>,
	M: Metrics,
{}

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
        let this = self.get_mut();

        if this.pos >= this.end {
            return Poll::Ready(None);
        }

        match &this.i {
            Some(Ok(i)) => {
                if this.future.is_none() {
                    let buffer = this.buffer;
                    let i = *i;
                    this.future = Some(Box::pin(async move { buffer.get(i).await }));
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


#[cfg(test)]
mod tests {
    use super::*;
    use tokio_stream::iter;

	type Result = std::result::Result<char, &'static str>;
	#[allow(type_alias_bounds)]
	type Buffer<I: Iterator<Item = Result>> = AsyncSourceBuffer<&'static str, tokio_stream::Iter<I>, crate::DefaultMetrics>;

    fn buffer<I: Iterator<Item = Result>, A: IntoIterator<IntoIter = I, Item = Result>>(input: A) -> Buffer<I> {
        AsyncSourceBuffer::new(
            iter(input.into_iter()),
            Position::default(),
            crate::DEFAULT_METRICS,
        )
    }

    #[tokio::test]
    async fn get_reads_characters_lazily() {
        let buffer = buffer("abc\ndef".chars().map(Ok));

        assert_eq!(buffer.get(0).await.unwrap(), Some('a'));
        assert_eq!(buffer.get(1).await.unwrap(), Some('b'));
        assert_eq!(buffer.get(2).await.unwrap(), Some('c'));
        assert_eq!(buffer.get(5).await.unwrap(), Some('e'));
    }

    #[tokio::test]
    async fn get_returns_none_at_eof() {
        let buffer = buffer("abc".chars().map(Ok));

        assert_eq!(buffer.get(0).await.unwrap(), Some('a'));
        assert_eq!(buffer.get(2).await.unwrap(), Some('c'));
        assert_eq!(buffer.get(3).await.unwrap(), None);
        assert_eq!(buffer.get(10).await.unwrap(), None);
    }

    #[tokio::test]
    async fn get_preserves_final_line_without_newline() {
        let buffer = buffer("abc".chars().map(Ok));

        assert_eq!(buffer.get(2).await.unwrap(), Some('c'));
        assert_eq!(buffer.get(0).await.unwrap(), Some('a'));
        assert_eq!(buffer.get(3).await.unwrap(), None);
    }

    #[tokio::test]
    async fn index_at_maps_positions_to_character_indices() {
        let buffer = buffer("abc\ndef".chars().map(Ok));

        assert_eq!(
            buffer.index_at(Position::new(0, 0)).await.unwrap(),
            Some(0)
        );
        assert_eq!(
            buffer.index_at(Position::new(0, 2)).await.unwrap(),
            Some(2)
        );
        assert_eq!(
            buffer.index_at(Position::new(1, 0)).await.unwrap(),
            Some(4)
        );
        assert_eq!(
            buffer.index_at(Position::new(1, 2)).await.unwrap(),
            Some(6)
        );
    }

    #[tokio::test]
    async fn index_at_returns_none_for_out_of_range_positions() {
        let buffer = buffer("abc\n".chars().map(Ok));

        assert_eq!(
            buffer.index_at(Position::new(0, 4)).await.unwrap(),
            None
        );
        assert_eq!(
            buffer.index_at(Position::new(5, 0)).await.unwrap(),
            None
        );
        assert_eq!(
            buffer.index_at(Position::new(0, 0)).await.unwrap(),
            Some(0)
        );
    }

    #[tokio::test]
    async fn at_returns_character_at_position() {
        let buffer = buffer("abc\ndef".chars().map(Ok));

        assert_eq!(
            buffer.at(Position::new(0, 1)).await.unwrap(),
            Some('b')
        );
        assert_eq!(
            buffer.at(Position::new(1, 2)).await.unwrap(),
            Some('f')
        );
        assert_eq!(
            buffer.at(Position::new(5, 0)).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn errors_are_returned_once() {
        let buffer = buffer([
            Ok('a'),
            Ok('\n'),
            Err("input error"),
        ]);

        assert_eq!(buffer.get(0).await.unwrap(), Some('a'));
        assert_eq!(buffer.get(2).await, Err("input error"));
        assert_eq!(buffer.get(2).await.unwrap(), None);
    }

    #[tokio::test]
    async fn iter_reads_entire_stream() {
        let buffer = buffer("abc\ndef".chars().map(Ok));

        let text = buffer.iter().await
			.into_string().await
			.unwrap();

        assert_eq!(text, "abc\ndef");
    }

    #[tokio::test]
    async fn iter_from_starts_at_requested_position() {
        let buffer = buffer("abc\ndef".chars().map(Ok));

        let text = buffer
            .iter_from(Position::new(1, 1)).await
            .into_string().await
            .unwrap();

        assert_eq!(text, "ef");
    }
}

