from datetime import datetime, timedelta
import re
from prompt_toolkit import PromptSession
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.validation import Validator, ValidationError
from prompt_toolkit import print_formatted_text as print
from loguru import logger
import api_vars

TO_LIST = lambda v: ''.join(list(v)).replace('\n', ' ').strip().split(' ')
CLI = PromptSession()


class VarValidator(Validator):
    """ 
    This class validates each variable at runtime.
    #TODO: docstring
    """
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

    def _not_digit(self, text: str):
        digits = list(text.replace('\n', '').strip('-').strip())
        for d in digits:
            if not d.isdigit():
                if d == '-':
                    continue
                else:
                    return f'`{d}` is not a valid digit'


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
                    variables = TO_LIST(var)
                    for v in variables:
                        if v not in self.vars['variable']:
                            error.message += ' Type only a variable name by line.'
                            raise error
            case 'year':
                years = []
                if not var:
                    pass
                elif '-' in var:
                    years = api_vars.str_range(var)
                else:
                    years = TO_LIST(var)
                if self._not_digit(var):
                    error.message += self._not_digit(var)
                    raise error                
                for year in years:
                    if year not in self.vars['year']:
                        error.message = (
                            f'`{year}` is invalid, please select between'
                            f' {min(api_vars.YEAR)} and {max(api_vars.YEAR)}.'
                        )
                        raise error
            case 'month':
                months = []
                if not var:
                    pass
                elif '-' in var:
                    months = api_vars.str_range(var)
                else:
                    months = TO_LIST(var)
                if var == 'all' or var.replace('\n', '') == 'all':
                    pass
                elif self._not_digit(var):
                    error.message += self._not_digit(var)
                    raise error
                else:
                    for month in months:
                        if f'{int(month):02d}' not in self.vars['month']:
                            error.message = (
                                f'`{month}` is invalid, please select between'
                                f' {min(api_vars.MONTH)} and {max(api_vars.MONTH)}.'
                            )
                            raise error
            case 'day':
                days = []
                if not var:
                    pass
                elif '-' in var:
                    days = api_vars.str_range(var)
                else:
                    days = TO_LIST(var)
                if var == 'all' or var.replace('\n', '') == 'all':
                    pass
                elif self._not_digit(var):
                    error.message += self._not_digit(var)
                    raise error   
                else:                    
                    for day in days:
                        if f'{int(day):02d}' not in self.vars['day']:
                            error.message = (
                                f'`{day}` is invalid, please select between'
                                f' {min(api_vars.DAY)} and {max(api_vars.DAY)}.'
                            )
                            raise error
            case 'time':
                if not var:
                    pass
                elif var == 'all' or var.replace('\n', '') == 'all':
                    pass
                else:
                    times = _time_regex_search(var)
                    if times:
                        for time in times:
                            if time not in self.vars['time']:
                                error.message = (
                                    f'{time} is invalid, please select between'
                                    f' {min(api_vars.TIME)} and {max(api_vars.TIME)}'
                                )
                                raise error
                    else:
                        error.message = (
                            f'{var} expression is incorrect. Format: `ini:end, _ hours`'
                        )



def reanalysis_cli():
    product_type = _product_type_prompt()
    variable = _variable_prompt()
    year = _year_prompt()
    month = _month_prompt(year)
    day = _day_prompt()
    time = _time_prompt()
    print(product_type)
    print(variable)
    print(year)
    print(month)
    print(day)
    print(time)
    reanalysis_cli()


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

    @property
    def last_update(self):
        return self.delay.date().strftime('%Y-%m-%d')


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
    vars: list = api_vars.VARIABLE

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
        rprompt=('Select a year range using (-): 2020-2023')
    )

    if not session:
        return default
    
    elif '-' in session:
        return api_vars.str_range(str(session).replace('\n', '').strip())

    else:
        years = TO_LIST(session)
        if len(years) == 1:
            return str(years[0])
        return years


def _month_prompt(year=None):
    vars = api_vars.MONTH
    default = DefaultDate().month

    def prompt_continuation(width, line_number, is_soft_wrap):
        return ('> ')

    session = CLI.prompt(
        f"Select the Month(s) ['{default}']:\n> ",
        completer=WordCompleter(vars),
        complete_while_typing=True,
        placeholder=default,
        validator=VarValidator('month'),
        validate_while_typing=False,
        multiline=True,
        prompt_continuation=prompt_continuation,
        mouse_support=True,
        rprompt=('Type `all` to select the entire year')
    )

    if not session:
        return default
    
    elif session == 'all' or session.replace('\n', '') == 'all':
        if year == DefaultDate().year:
            year_months = api_vars.str_range(f'01-{DefaultDate().month}')
            if len(year_months) == 1:
                return f'{int(year_months[0]):02d}'
            return year_months
        return vars

    elif '-' in session:
        months = api_vars.str_range(str(session).replace('\n', '').strip())
        return sorted(list(map(lambda s: f'{int(s):02d}', months)))

    else:
        months = TO_LIST(session)
        if len(months) == 1:
            return f'{int(months[0]):02d}'
        return sorted(list(map(lambda s: f'{int(s):02d}', months)))


def _day_prompt(month=None):
    vars = api_vars.DAY
    default = DefaultDate().day

    def prompt_continuation(width, line_number, is_soft_wrap):
        return ('> ')

    session = CLI.prompt(
        f"Select the Day(s) ['{default}']:\n> ",
        completer=WordCompleter(vars),
        complete_while_typing=True,
        placeholder=default,
        validator=VarValidator('day'),
        validate_while_typing=False,
        multiline=True,
        prompt_continuation=prompt_continuation,
        mouse_support=True,
        rprompt=('Select a days range using (-): 1-31')
    )

    if not session:
        return default

    elif session == 'all' or session.replace('\n', '') == 'all':
        ... # calculate month days
    
    elif '-' in session:
        days = api_vars.str_range(str(session).replace('\n', '').strip())
        return sorted(list(map(lambda s: f'{int(s):02d}', days)))

    else:
        days = TO_LIST(session)
        if len(days) == 1:
            return f'{int(days[0]):02d}'
        return sorted(list(map(lambda s: f'{int(s):02d}', days)))


def _time_prompt():
    vars = api_vars.TIME
    default = [
        '00:00',
        '03:00',
        '06:00',
        '09:00',
        '12:00',
        '15:00',
        '18:00',
        '21:00',
    ]

    def prompt_continuation(width, line_number, is_soft_wrap):
        return ('> ')
    
    session = CLI.prompt(
        f"Select the Hour(s) or Interval {default}:\n> ",
        completer=WordCompleter(vars),
        complete_while_typing=True,
        placeholder='',
        validator=VarValidator('time'),
        validate_while_typing=False,
        multiline=True,
        prompt_continuation=prompt_continuation,
        mouse_support=True,
        rprompt=('Type the hour or select skipping `_ hours`. Default: `3 hours`')
    )

    if not session:
        return default

    elif session == 'all' or session.replace('\n', '') == 'all':
        return vars
    
    else:
        times = _time_regex_search(session)
        if len(times) == 1:
            return times[0]
        return times


def _time_regex_search(text: str):
    times: list = api_vars.TIME

    regex = re.compile(
        '^[\n]*?((\d?\d:\d\d[\n]?)*?-?(\d\d:\d\d?)?)?,? ?(\d\d?)? ?[hours]*?[\n]*?$'
    )

    matches: list = re.match(regex, text).groups()

    if any(matches):
        if '\n' in matches[0][2:-2]:
            return sorted(list(set(str(matches[0]).split('\n'))))
        expr = [match for match in matches[1:] if match]
        if len(expr) == 1:
            if expr[0] in times:
                return list(expr[0])
            elif str(expr[0]).isdigit():
                return times[::int(expr[0])]
            else:
                return None
        elif len(expr) == 2:
            if expr[0] in times and expr[1] in times:
                ini: int = times.index(expr[0])
                end: int = times.index(expr[1])
                if ini < end:
                    return times[ini:end+1] # Inclusive
                elif ini > end:
                    res = []
                    res.extend(times[ini:])
                    res.extend(times[:end+1])
                    return sorted(res)
                else:
                    return None
            else:
                return None
        elif len(expr) == 3:
            if expr[0] in times and expr[1] in times and str(expr[2]).isdigit():
                ini: int = times.index(expr[0])
                end: int = times.index(expr[1])
                step: int = int(expr[2])
                if ini < end:
                    return times[ini:end+1:step]
                elif ini > end:
                    res = []
                    res.extend(times[ini:])
                    res.extend(times[:end+1])
                    return sorted(res)[::step]
                else:
                    return None
            else:
                return None
        else:
            return None


if __name__ == "__main__":
    reanalysis_cli()
