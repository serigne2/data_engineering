import pandas as pd


def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    datetime_dim = df[['tpep_pickup_datetime','tpep_dropoff_datetime']].drop_duplicates().reset_index(drop=True)
    datetime_dim['pick_hour'] = datetime_dim['tpep_pickup_datetime'].dt.hour
    datetime_dim['pick_day'] = datetime_dim['tpep_pickup_datetime'].dt.day
    datetime_dim['pick_month'] = datetime_dim['tpep_pickup_datetime'].dt.month
    datetime_dim['pick_year'] = datetime_dim['tpep_pickup_datetime'].dt.year
    datetime_dim['pick_weekday'] = datetime_dim['tpep_pickup_datetime'].dt.weekday

    datetime_dim['drop_hour'] = datetime_dim['tpep_dropoff_datetime'].dt.hour
    datetime_dim['drop_day'] = datetime_dim['tpep_dropoff_datetime'].dt.day
    datetime_dim['drop_month'] = datetime_dim['tpep_dropoff_datetime'].dt.month
    datetime_dim['drop_year'] = datetime_dim['tpep_dropoff_datetime'].dt.year
    datetime_dim['drop_weekday'] = datetime_dim['tpep_dropoff_datetime'].dt.weekday

    datetime_dim['datetime_id'] = datetime_dim.index
    datetime_dim = datetime_dim[['datetime_id', 'tpep_pickup_datetime', 'pick_hour', 'pick_day', 'pick_month', 'pick_year', 'pick_weekday',
                             'tpep_dropoff_datetime', 'drop_hour', 'drop_day', 'drop_month', 'drop_year', 'drop_weekday']]

    #passenger_count_dim = df[['passenger_count']].drop_duplicates().reset_index(drop=True) à revoir
    # passenger_count_dim['passenger_count_id'] = passenger_count_dim.index
    # passenger_count_dim = passenger_count_dim[['passenger_count_id','passenger_count']]

    # trip_distance_dim = df[['trip_distance']].drop_duplicates().reset_index(drop=True) à revoir
    # trip_distance_dim['trip_distance_id'] = trip_distance_dim.index
    # trip_distance_dim = trip_distance_dim[['trip_distance_id','trip_distance']]
    rate_code_type = {
        1:"Standard rate",
        2:"JFK",
        3:"Newark",
        4:"Nassau or Westchester",
        5:"Negotiated fare",
        6:"Group ride"
    }

    rate_code_dim = df[['RatecodeID']].drop_duplicates().reset_index(drop=True)
    rate_code_dim['rate_code_id'] = rate_code_dim.index
    rate_code_dim['rate_code_name'] = rate_code_dim['RatecodeID'].map(rate_code_type)
    rate_code_dim = rate_code_dim[['rate_code_id','RatecodeID','rate_code_name']]

    
    store_dim = df[['store_and_fwd_flag']].drop_duplicates().reset_index(drop=True)
    store_dim['store_id'] = store_dim.index
    store_dim = store_dim[['store_id','store_and_fwd_flag']] 
    
    pickup_location_dim = df[['pickup_longitude', 'pickup_latitude']].drop_duplicates().reset_index(drop=True)
    pickup_location_dim['pickup_location_id'] = pickup_location_dim.index
    pickup_location_dim = pickup_location_dim[['pickup_location_id','pickup_latitude','pickup_longitude']] 
    
    dropoff_location_dim = df[['dropoff_longitude', 'dropoff_latitude']].drop_duplicates().reset_index(drop=True)
    dropoff_location_dim['dropoff_location_id'] = dropoff_location_dim.index
    dropoff_location_dim = dropoff_location_dim[['dropoff_location_id','dropoff_latitude','dropoff_longitude']]

    # dropoff_location_dim = df[['dropoff_longitude', 'dropoff_latitude']].drop_duplicates().reset_index(drop=True)
    # dropoff_location_dim['dropoff_location_id'] = dropoff_location_dim.index
    # dropoff_location_dim = dropoff_location_dim[['dropoff_location_id','dropoff_latitude','dropoff_longitude']]

    payment_type_name = {
        1:"Credit card",
        2:"Cash",
        3:"No charge",
        4:"Dispute",
        5:"Unknown",
        6:"Voided trip"
    }
    payment_type_dim = df[['payment_type']].drop_duplicates().reset_index(drop=True)
    payment_type_dim['payment_type_id'] = payment_type_dim.index
    payment_type_dim['payment_type_name'] = payment_type_dim['payment_type'].map(payment_type_name)
    payment_type_dim = payment_type_dim[['payment_type_id','payment_type','payment_type_name']]

    fact_table = df.merge(rate_code_dim, on='RatecodeID') \
             .merge(pickup_location_dim, on=['pickup_longitude', 'pickup_latitude']) \
             .merge(dropoff_location_dim, on=['dropoff_longitude', 'dropoff_latitude'])\
             .merge(datetime_dim, on=['tpep_pickup_datetime','tpep_dropoff_datetime']) \
             .merge(payment_type_dim, on='payment_type') \
             .merge(store_dim, on='store_and_fwd_flag')[['VendorID', 'datetime_id','rate_code_id', 'store_id', 'pickup_location_id', 'dropoff_location_id',
               'payment_type_id', 'fare_amount','trip_distance', 'passenger_count', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
               'improvement_surcharge', 'total_amount']]
    #print(fact_table.head())
    #Vefif1
    #print("Lignes avant merge :", len(df)) 
    #print("Lignes après merge :", len(fact_table))
    #vérif : Véerfier les clès non matchées(méthode robuste). Exemple je peux tester une dimension comme suit:
    #both: correspondance trouvée, left_only présent dans df mais pas dans dimension , 
    #right_only présent dans dimension mais pas dans df 
    test = df.merge(
           dropoff_location_dim,
           on=['dropoff_longitude','dropoff_latitude'],
           how='left',
           indicator=True
    )

    #print(test['_merge'].value_counts())
    #Verif3: vérifier les valeurs nulls après merge
    #print(fact_table.isnull().sum())
    #Vérfier les doublons problèmes silencieux:
    dropoff_location_dim.duplicated(
    subset=['dropoff_longitude','dropoff_latitude']
    ).sum()
    return {"datetime_dim":datetime_dim.to_dict(orient="dict"),
    "store_dim":store_dim.to_dict(orient="dict"),
    "rate_code_dim":rate_code_dim.to_dict(orient="dict"),
    "pickup_location_dim":pickup_location_dim.to_dict(orient="dict"),
    "dropoff_location_dim":dropoff_location_dim.to_dict(orient="dict"),
    "payment_type_dim":payment_type_dim.to_dict(orient="dict"),
    "fact_table":fact_table.to_dict(orient="dict")}
#df=pd.read_csv("/home/macbookair/data_engineering/uber-data-pipeline-project/data/raw/uber-data.csv")
#transformed_data = transform(df)


