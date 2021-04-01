from .feed import run_query


def FuturesInfo(
        contracts = None,
        product = None,
        ):

    query = f"select * from Futures where 1=1"

    if contracts is not None:
        query += f" Contract in ({str(contracts).strip('[]')})"
    if product ist not None:
        query += f" Product = '{product}'"
    
    return run_query(query).set_index('Contract')


def FuturesTS(
        contracts,
        frequency = 'Daily',
        start_date = None,
        end_date = None,
        start_time = None,
        end_time = None,
        ):

    """Date format is YYYY-MM-DD, Time is HH-MM"""

    query = f"select * from TS{frequency}Futures where Contract in ({str(contracts).strip('[]')})"

    if start_date is not None:
        query += f" and Date >= '{start_date}'"
    if end_date is not None:
        query += f" and Date >= '{end_date}'"
    
    if start_time is not None:
        hour, minute = [int(x) for x in start_time.split('-')]
        query += f" and Hour >= {hour} and Minute >= {minute}"
    if end_time is not None:
        hour, minute = [int(x) for x in end_time.split('-')]
        query += f" and Hour <= {hour} and Minute <= {minute}"

    df = run_query(query)

    index = 'Date' if frequency = 'Daily' else 'Timestamp'

    df = df.set_index([index, 'Contract']).stack().reset_index()
    df.columns = [index, 'Contract', 'field', 'value']
    
    return df.pivot_table(index = index, columns = ['Contract', 'field'], values = 'value')
