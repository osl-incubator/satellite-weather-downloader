from typing_extensions import Annotated
from pydantic.functional_validators import AfterValidator

from satellite.weather.orm import validations as v

ADM0Options = Annotated[str, AfterValidator(v.validate_adm0_options)]
DatasetFile = Annotated[str, AfterValidator(v.validate_xr_dataset_file_path)]
