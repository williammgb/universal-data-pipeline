class UdpError(Exception):
    """Base class for errors the platform raises on purpose."""


class ConfigError(UdpError):
    """A source definition is missing or invalid."""


class ExtractError(UdpError):
    """A source could not be read."""


class ValidationError(UdpError):
    """Extracted data does not have the shape the pipeline requires."""


class LoadError(UdpError):
    """Data could not be written to the platform database."""
