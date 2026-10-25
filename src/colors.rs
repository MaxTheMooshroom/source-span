
#[cfg(feature = "crossterm")]
use crossterm::style::Attribute;

use std::fmt::{Display, Formatter, Result as FmtResult};

use crate::fmt::Color;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct Foreground(Color);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct Background(Color);

impl From<Color> for Foreground {
	fn from(value: Color) -> Self {
		Self(value)
	}
}

impl From<Color> for Background {
	fn from(value: Color) -> Self {
		Self(value)
	}
}

impl Display for Foreground {
	#[cfg(feature = "termion")]
	fn fmt(&self, f: &mut Formatter) -> FmtResult {
		<Color as termion::color::Color>::write_fg(&self.0, f)
	}

	#[cfg(feature = "crossterm")]
	fn fmt(&self, f: &mut Formatter) -> FmtResult {
		crossterm::style::SetForegroundColor(self.0.into()).fmt(f)
	}

	#[cfg(not(feature = "colors-backend"))]
	fn fmt(&self, _: &mut Formatter) -> FmtResult {
		Ok(())
	}
}

impl Display for Background {
	#[cfg(feature = "termion")]
	fn fmt(&self, f: &mut Formatter) -> FmtResult {
		<Color as termion::color::Color>::write_bg(&self.0, f)
	}

	#[cfg(feature = "crossterm")]
	fn fmt(&self, f: &mut Formatter) -> FmtResult {
		crossterm::style::SetBackgroundColor(self.0.into()).fmt(f)
	}

	#[cfg(not(feature = "colors-backend"))]
	fn fmt(&self, _: &mut Formatter) -> FmtResult {
		Ok(())
	}
}

#[cfg(feature = "crossterm")]
macro_rules! select_ident {
	([$ct_style:ident] $this:ident) => { Attribute::$ct_style };
	($style:ident) => { Attribute::$style };
}

macro_rules! style_impls {
	($($([$ct_style:ident])? $style:ident),* $(,)?) => {
		#[derive(Clone, Copy, PartialEq, Eq, Debug)]
		#[allow(dead_code)]
		pub(crate) enum Style { $($style),* }

		impl Style {
			#[cfg(feature = "crossterm")]
			pub fn into_attribute(self) -> Attribute {
				match self {
					$(Self::$style => select_ident!($([$ct_style])? $style)),*
				}
			}
		}

		impl std::fmt::Display for Style {
			#[cfg(feature = "termion")]
			fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
				match self {
					$(Self::$style => termion::style::$style.fmt(f)),*
				}
			}

			#[cfg(feature = "crossterm")]
			fn fmt(&self, f: &mut Formatter) -> FmtResult {
				self.into_attribute().fmt(f)
			}

			// no-op because there's no color backend to use for reference
			#[cfg(not(feature = "colors-backend"))]
			fn fmt(&self, _: &mut std::fmt::Formatter) -> std::fmt::Result {
				Ok(())
			}
		}
	};
}

style_impls![
						Reset,
						Bold,
	[Dim] 				Faint,
						Italic,
	[Underlined] 		Underline,
	[SlowBlink] 		Blink,
	[Reverse] 			Invert,
						CrossedOut,
						NoBold,
	[NormalIntensity] 	NoFaint,
						NoItalic,
						NoUnderline,
						NoBlink,
	[NoReverse] 		NoInvert,
	[NotCrossedOut] 	NoCrossedOut,
						Framed,
];
