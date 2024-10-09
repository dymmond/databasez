from __future__ import annotations

from functools import cached_property
from typing import Any
from urllib.parse import SplitResult, parse_qs, quote, unquote, urlencode, urlsplit

from sqlalchemy import URL
from sqlalchemy.engine import make_url


class DatabaseURL:
    def __init__(self, url: str | DatabaseURL | URL | None = None):
        if isinstance(url, DatabaseURL):
            self._url: str = url._url
        elif isinstance(url, URL):
            self._url = url.render_as_string(hide_password=False)
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
        url = self.sqla_url
        _components = urlsplit(url.render_as_string(hide_password=False))
        if _components.path.startswith("///"):
            _components = _components._replace(path=f"//{_components.path.lstrip('/')}")
        return _components

    @classmethod
    def get_url(cls, splitted: SplitResult) -> str:
        url = f"{splitted.scheme}://{(splitted.netloc or '')}{splitted.path}"
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
    def driver(self) -> str | None:
        splitted = self.scheme.split("+", 1)
        if len(splitted) == 1:
            return None
        return splitted[1]

    @property
    def userinfo(self) -> bytes | None:
        if self.components.username:
            info = quote(self.components.username, safe="+")
            if self.password:
                info += ":" + quote(self.password, safe="+")
            return info.encode("utf-8")
        return None

    @property
    def username(self) -> str | None:
        if self.components.username is None:
            return None
        return unquote(self.components.username)

    @property
    def password(self) -> str | None:
        if self.components.password is None:
            return None
        return unquote(self.components.password)

    @property
    def hostname(self) -> str | None:
        host = self.components.hostname or self.options.get("host")
        if isinstance(host, list):
            if len(host) > 0:
                return host[0]
            return None
        else:
            return host

    @property
    def port(self) -> int | None:
        return self.components.port

    @property
    def netloc(self) -> str | None:
        return self.components.netloc

    @property
    def database(self) -> str:
        path = self.components.path
        if path.startswith("/"):
            path = path[1:]
        return unquote(path)

    @cached_property
    def options(self) -> dict[str, str | list[str]]:
        result: dict[str, str | list[str]] = {}
        for key, val in parse_qs(self.components.query).items():
            if len(val) == 1:
                result[key] = val[0]
            else:
                result[key] = val
        return result

    def replace(self, **kwargs: Any) -> DatabaseURL:
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
                userpass = quote(username, safe="+")
                if password is not None:
                    userpass += f":{quote(password, safe='+')}"
                netloc = f"{userpass}@{netloc}"

            kwargs["netloc"] = netloc

        if "database" in kwargs:
            database = kwargs.pop("database")
            if database is None:
                kwargs["path"] = ""
            else:
                # pathes should begin with /
                kwargs["path"] = f"/{database}"

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

    @cached_property
    def obscure_password(self) -> str:
        return self.sqla_url.render_as_string(hide_password=True)

    @cached_property
    def sqla_url(self) -> URL:
        return make_url(self._url)

    def upgrade(self, **extra_options: Any) -> DatabaseURL:
        from .database import Database

        return Database.apply_database_url_and_options(self, **extra_options)[1]

    def __str__(self) -> str:
        return self.get_url(self.components)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({repr(self.obscure_password)})"

    def __eq__(self, other: Any) -> bool:
        # fix encoding
        if isinstance(other, str):
            other = DatabaseURL(other)
        return str(self) == str(other)
