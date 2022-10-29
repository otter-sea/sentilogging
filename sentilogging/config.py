import pkg_resources
from dynaconf import Dynaconf

settings = Dynaconf(envvar_prefix="DYNACONF", settings_files=[pkg_resources.resource_filename(
    __name__, "settings.toml"), pkg_resources.resource_filename(__name__, ".secrets.toml")])

