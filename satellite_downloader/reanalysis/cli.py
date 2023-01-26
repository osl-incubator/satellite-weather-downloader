from datetime import datetime, timedelta
from prompt_toolkit import PromptSession
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.validation import Validator, ValidationError
from prompt_toolkit import print_formatted_text as print
import api_vars

TO_LIST = lambda v: ''.join(list(v)).replace('\n', ' ').strip().split(' ')
CLI = PromptSession()


class VarValidator(Validator):
    var_type: str = None

    vars = dict(
        product_type = api_vars.PRODUCT_TYPE,
        variable = api_vars.VARIABLE,
        year = api_vars.YEAR,
        month = api_vars.MONTH,
        day = api_vars.DAY,
        time = api_vars.TIME,
        format = api_vars.FORMAT
    )

    def __init__(self, var_type):
        self.var_type = var_type


    def validate(self, document):
        var = document.text
        error = ValidationError(
            message='Invalid input. Press <Tab> to display all options.',
            cursor_position=0
        )

        match self.var_type:
            case 'product_type':
                if var and var not in self.vars['product_type']:
                    raise error
                elif not var:
                    pass
            case 'variable':
                if not var:
                    pass
                else:
                    vars = TO_LIST(var)
                    for v in vars:
                        if v not in self.vars['variable']:
                            error.message += ' Type only a variable name by line.'
                            raise error
            case 'year':
                if not var:
                    pass
                elif '-' in var:
                    digits = list(var.strip('\n').strip('-').strip())
                    for d in digits:
                        if not d.isdigit():
                            if d == '-':
                                continue
                            else:
                                error.message = f'`{d}` is not a valid digit'
                                raise error
                    years = api_vars.years_range(var)
                    for year in years:
                        if year not in self.vars['year']:
                            error.message = (
                                'Invalid date range, please select years between'
                                f' {min(api_vars.YEAR)} and {max(api_vars.YEAR)}.'
                            )
                            raise error
                else:
                    years = TO_LIST(var)
                    for year in years:
                        if year not in self.vars['year']:
                            error.message += ' Type only a year by line.'
                            raise error



def reanalysis_cli():
    product_type = _product_type_prompt()
    variable = _variable_prompt()
    year = _year_prompt()
    print(product_type)
    print(variable)
    print(year)


class DefaultDate:
    """ 
    Ensures the default date is available to download.
    The Copernicus delays between 5 to 8 days to update
    it's database.
    This method returns the (year, month and date) - 9
    days from present's date.
    """
    today = datetime.now()
    delay = today - timedelta(days=9)
    
    @property
    def year(self):
        return f'{self.delay.year}'
    
    @property
    def month(self):
        return f'{self.delay.month:02d}'

    @property
    def day(self):
        return f'{self.delay.day:02d}'


def _product_type_prompt():
    vars = api_vars.PRODUCT_TYPE
    default = 'reanalysis'

    def prompt_continuation(width, line_number, is_soft_wrap):
        return ('> ')

    session = CLI.prompt(
        f"Select the Product Type ['{default}']:\n> ",
        completer=WordCompleter(vars),
        complete_while_typing=True,
        placeholder=default,
        validator=VarValidator('product_type'),
        validate_while_typing=False,
        multiline=False,
        prompt_continuation=prompt_continuation,
    )

    if not session:
        return default

    return session


def _variable_prompt():
    vars = api_vars.VARIABLE
    defaults = [
        '2m_temperature',
        'total_precipitation',
        '2m_dewpoint_temperature',
        'mean_sea_level_pressure',
    ]

    def prompt_continuation(width, line_number, is_soft_wrap):
        return ('> ')

    session = CLI.prompt(
        f'Select the Variable(s) {defaults}:\n> ',
        completer=WordCompleter(vars),
        complete_while_typing=True,
        placeholder='',
        validator=VarValidator('variable'),
        validate_while_typing=False,
        multiline=True,
        prompt_continuation=prompt_continuation,
        mouse_support=True,
        rprompt='Press <Alt> + <Enter> to finish selection'
    )

    if not session:
        return defaults
    
    else:
        variables = TO_LIST(session)
        if len(variables) == 1:
            return str(variables[0])
        return variables


def _year_prompt():
    vars = api_vars.YEAR
    default = DefaultDate().year

    def prompt_continuation(width, line_number, is_soft_wrap):
        return ('> ')

    session = CLI.prompt(
        f"Select the Year(s) ['{default}']:\n> ",
        completer=WordCompleter(vars),
        complete_while_typing=True,
        placeholder=default,
        validator=VarValidator('year'),
        validate_while_typing=False,
        multiline=True,
        prompt_continuation=prompt_continuation,
        mouse_support=True,
        rprompt=('You can select a range using (-): 2020-2023')
    )

    if not session:
        return default
    
    elif '-' in session:
        return api_vars.years_range(str(session).replace('\n', '').strip())

    else:
        years = TO_LIST(session)
        if len(years) == 1:
            return str(years[0])
        return years


if __name__ == "__main__":
    reanalysis_cli()
