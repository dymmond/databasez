import typing
from functools import cached_property
from urllib.parse import SplitResult, parse_qsl, quote, unquote, urlencode, urlsplit

from sqlalchemy import URL
from sqlalchemy.engine import make_url


class DatabaseURL:
    def __init__(self, url: typing.Union[str, "DatabaseURL", None] = None):
        if isinstance(url, DatabaseURL):
            self._url: str = url._url
        elif isinstance(url, str):
            self._url = url
        elif url is None:
            self._url = "invalid://localhost"
        else:
            raise TypeError(
                f"Invalid type for DatabaseURL. Expected str or DatabaseURL, got {type(url)}"
            )

    @cached_property
    def components(self) -> SplitResult:
        url = make_url(self._url)
        _components = urlsplit(url.render_as_string(hide_password=False))
        if _components.path.startswith("///"):
            _components = _components._replace(path=f"//{_components.path.lstrip('/')}")
        return _components

    @classmethod
    def get_url(cls, splitted: SplitResult) -> str:
        url = f"{splitted.scheme}://{splitted.netloc or  ""}{splitted.path}"
        if splitted.query:
            url = f"{url}?{splitted.query}"
        if splitted.fragment:
            url = f"{url}#{splitted.fragment}"
        return url

    @property
    def scheme(self) -> str:
        return self.components.scheme

    @property
    def dialect(self) -> str:
        return self.scheme.split("+")[0]

    @property
    def driver(self) -> typing.Optional[str]:
        splitted = self.scheme.split("+", 1)
        if len(splitted) == 1:
            return None
        return splitted[1]

    @property
    def userinfo(self) -> typing.Optional[bytes]:
        if self.components.username:
            info = self.components.username
            if self.password:
                info += ":" + quote(self.password)
            return info.encode("utf-8")
        return None

    @property
    def username(self) -> typing.Optional[str]:
        if self.components.username is None:
            return None
        return unquote(self.components.username)

    @property
    def password(self) -> typing.Optional[str]:
        if self.components.password is None:
            return None
        return self.components.password

    @property
    def hostname(self) -> typing.Optional[str]:
        return (
            self.components.hostname or self.options.get("host") or self.options.get("unix_sock")
        )

    @property
    def port(self) -> typing.Optional[int]:
        return self.components.port

    @property
    def netloc(self) -> typing.Optional[str]:
        return self.components.netloc

    @property
    def database(self) -> str:
        path = self.components.path
        if path.startswith("/"):
            path = path[1:]
        return unquote(path)

    @property
    def options(self) -> dict:
        if not hasattr(self, "_options"):
            self._options = dict(parse_qsl(self.components.query))
        return self._options

    def replace(self, **kwargs: typing.Any) -> "DatabaseURL":
        if (
            "username" in kwargs
            or "user" in kwargs
            or "password" in kwargs
            or "hostname" in kwargs
            or "host" in kwargs
            or "port" in kwargs
        ):
            hostname = kwargs.pop("hostname", kwargs.pop("host", self.hostname))
            port = kwargs.pop("port", self.port)
            username = kwargs.pop("username", kwargs.pop("user", self.components.username))
            password = kwargs.pop("password", self.components.password)

            netloc = hostname
            if port is not None:
                netloc += f":{port}"
            if username is not None:
                userpass = username
                if password is not None:
                    userpass += f":{password}"
                netloc = f"{userpass}@{netloc}"

            kwargs["netloc"] = netloc

        if "database" in kwargs:
            # pathes should begin with /
            kwargs["path"] = "/" + kwargs.pop("database")

        if "dialect" in kwargs or "driver" in kwargs:
            dialect = kwargs.pop("dialect", self.dialect)
            driver = kwargs.pop("driver", self.driver)
            kwargs["scheme"] = f"{dialect}+{driver}" if driver else dialect

        if not kwargs.get("netloc", self.netloc):
            kwargs["netloc"] = ""
        if "options" in kwargs:
            kwargs["query"] = urlencode(kwargs.pop("options"), doseq=True)

        components = self.components._replace(**kwargs)
        return self.__class__(self.get_url(components))

    @property
    def obscure_password(self) -> str:
        if self.password:
            return self.replace(password="********")._url
        return str(self)

    @cached_property
    def sqla_url(self) -> URL:
        return make_url(str(self))

    def upgrade(self, **extra_options: typing.Any) -> "DatabaseURL":
        from .database import Database

        return Database.apply_database_url_and_options(self, **extra_options)[1]

    def __str__(self) -> str:
        return self.get_url(self.components)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({repr(self.obscure_password)})"

    def __eq__(self, other: typing.Any) -> bool:
        # fix encoding
        if isinstance(other, str):
            other = DatabaseURL(other)
        return str(self) == str(other)
