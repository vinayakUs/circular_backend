

import io
import os
from pathlib import Path
from random import randint
from typing import override

from captcha.image import ImageCaptcha

from app.auth.captcha.renderers.base import ChallengeRenderer


class DistortedTextRenderer(ChallengeRenderer):
    """Default CAPTCHA renderer built on the ``captcha`` library.

    The library generates a PNG with random per-character rotation,
    a wavy curve drawn across the text, configurable noise dots,
    and configurable noise lines.  These are exactly the techniques
    we were hand-rolling with ``ImageDraw.point``, ``Image.arc``,
    and a custom 6x2 mesh warp.
    """

    def __init__(
            self,
            font_file: str | os.PathLike,
            extra_curves: int = 2
    ):
        path = Path(font_file)
        if not path.is_file():
            raise FileNotFoundError(f"Font file not found: {path}")
        self._font_file = str(path)
        self._extra_curves = extra_curves

    def _get_captcha(self, width: int, height: int) -> ImageCaptcha:
        return ImageCaptcha(
                width=width,
                height=height,
                fonts=[self._font_file],
                font_sizes=(int(height * 0.6), int(height * 0.7), int(height * 0.8)),
            )
        
        

    @override
    def render(
            self,
            answer: str,
            width: int,
            height: int,
        ) -> bytes:

        gen = self._get_captcha(width, height)
        image = gen.generate_image(answer)
        for _ in range(self._extra_curves):
            gen.create_noise_curve(image, (randint(40, 120),) * 3)
        buf = io.BytesIO()
        image.save(buf, format="PNG", optimize=True)
        return buf.getvalue()

    
    @property
    @override
    def content_type(self) -> str:
        return "image/png"
