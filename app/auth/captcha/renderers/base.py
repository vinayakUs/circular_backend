"""
ABC for CAPTCHA strategies (answer generator + visual renderer).
""" 


from abc import ABC, abstractmethod


class AnswerGenerator(ABC):
    """
    Family-of-related-objects factory:
    produces random answer strings.
    """

    @abstractmethod
    def generate(self, length: int) -> str:
        """
        Return a fresh random answer of the given length.

        Implementations must use a CSPRNG (`secrets.choice`)
        and exclude visually-confusable characters from the alphabet.
        """


class ChallengeRenderer(ABC):
    """
    Strategy for turning an answer string into bytes
    shown to the user.
    """

    @abstractmethod
    def render(
        self,
        answer: str,
        width: int,
        height: int,
    ) -> bytes:
        """
        Render the answer as a CAPTCHA image.

        Returns:
            Image bytes.
        """

    @property
    @abstractmethod
    def content_type(self) -> str:
        """
        HTTP Content-Type the renderer produces,
        e.g. 'image/png'.
        """