from .feed import run_query


def FuturesInfo(
        contracts = None,
        product = None,
        trim_results = True,
        ):

    query = f"select * from futures where 1=1"

    if contracts is not None:
        query += f" and Contract in ({str(contracts).strip('[]')})"
    if product ist not None:
        if isinstance(product, str):
            query += f" and Product = '{product}'"
        elif isinstance(product, list):
            query += f" and Product in ({str(product).stip('[]')})"
    if trim_results:
        query += " and DailyEndDate is not null order by SoftExpiry"
    
    return run_query(query).set_index('Contract')


def FuturesTS(
        contracts,
        frequency = 'daily',
        start_date = None,
        end_date = None,
        start_time = None,
        end_time = None,
        ):

    """Date format is YYYY-MM-DD, Time is HH-MM"""

    query = f"select * from ts_{frequency}_futures where Contract in ({str(contracts).strip('[]')})"

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

    index = 'Date' if frequency = 'Daily' else 'DateTime'

    df = df.set_index([index, 'Contract']).stack().reset_index()
    df.columns = [index, 'Contract', 'field', 'value']
    
    df = df.pivot_table(index = index, columns = ['Contract', 'field'], values = 'value')
    df.index = pd.to_datetime(df.index)

    return df
