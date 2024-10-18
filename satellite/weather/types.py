from typing import Optional

from typing_extensions import Annotated
from pydantic.functional_validators import AfterValidator

from satellite.weather.validations import *  # noqa

Locale = Annotated[Optional[str], AfterValidator(validate_locale)]
DatasetFile = Annotated[str, AfterValidator(validate_xr_dataset_file_path)]
