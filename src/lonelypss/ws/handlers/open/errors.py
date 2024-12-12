import sys
from typing import List, Union, cast


class NormalDisconnectException(Exception): ...


class AuthRejectedException(Exception): ...


if sys.version_info < (3, 11):

    def combine_multiple_base_exceptions(
        msg: str, excs: List[BaseException]
    ) -> BaseException:
        """Raises a single BaseException whose __cause__ includes all
        the indicate exceptions and their causes.
        """
        if not excs:
            raise ValueError("no exceptions to combine")

        if len(excs) == 1:
            return excs[0]

        exc = BaseException(msg)
        last_exc = exc

        for nexc in excs:
            while last_exc.__cause__ is not None:
                last_exc = last_exc.__cause__
            last_exc.__cause__ = nexc

        return exc

    def combine_multiple_normal_exceptions(
        msg: str, excs: List[Exception]
    ) -> Exception:
        """Raises a single Exception whose __cause__ includes all
        the indicate exceptions and their causes.
        """
        if not excs:
            raise ValueError("no exceptions to combine")

        if len(excs) == 1:
            return excs[0]

        exc = Exception(msg)
        last_exc: Union[Exception, BaseException] = exc

        for nexc in excs:
            while last_exc.__cause__ is not None:
                last_exc = last_exc.__cause__
            last_exc.__cause__ = nexc

        return exc

else:

    def combine_multiple_base_exceptions(
        msg: str, excs: List[BaseException]
    ) -> BaseException:
        """Light wrapper around BaseExceptionGroup"""
        if not excs:
            raise ValueError("no exceptions to combine")

        if len(excs) == 1:
            return excs[0]

        if any(isinstance(e, BaseExceptionGroup) for e in excs):
            new_excs: List[BaseException] = []
            for e in excs:
                if isinstance(e, BaseExceptionGroup):
                    new_excs.extend(e.exceptions)
                else:
                    new_excs.append(e)
            excs = new_excs

        return BaseExceptionGroup(msg, excs)

    def combine_multiple_normal_exceptions(
        msg: str, excs: List[Exception]
    ) -> Exception:
        """Light wrapper around ExceptionGroup"""
        if not excs:
            raise ValueError("no exceptions to combine")

        if len(excs) == 1:
            return excs[0]

        if any(isinstance(e, ExceptionGroup) for e in excs):
            new_excs: List[Exception] = []
            for e in excs:
                if isinstance(e, ExceptionGroup):
                    new_excs.extend(e.exceptions)
                else:
                    new_excs.append(e)
            excs = new_excs

        return ExceptionGroup(msg, excs)


def combine_multiple_exceptions(msg: str, excs: List[BaseException]) -> BaseException:
    """Returns a single Exception, if possible, otherwise a BaseException which will
    report all the indicated exceptions in the most informative way possible.
    """
    if all(isinstance(e, Exception) for e in excs):
        return combine_multiple_normal_exceptions(msg, cast(List[Exception], excs))
    return combine_multiple_base_exceptions(msg, excs)
