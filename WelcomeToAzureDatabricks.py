curr_envs= ['Azure Databricks Community', 'Azure Databricks WS Env']

sample_data = spark.createDataFrame(
    [
        (101102103104, 'XX_Active', 123.50, True, 'Out_Of_Stock'),
        (101102103105, 'XY_Passive', 59.99, False, 'Out_Of_Stock'),
        (101102103106, 'YX_Active', 120.99, False, 'In_Stock'),
        (101102103107, 'ZZ_Active', 44.99, True, 'In_Stock'),
        (101102103108, 'SF_Passive', 19.99, False, 'In_Stock'),
        (101102103109, 'GG_Active', 99.99, False, 'Out_Of_Stock'),
        (101102103110, 'GD_Passive', 111.99, True, 'In_Stock'),
        (101102103111, 'AS_Active', 12.99, False, 'Out_Of_Stock'),
        (101102103112, 'JF_Active', 32.99, False, 'In_Stock'),
        (101102103113, 'ER_Passive', 123.39, True, 'Out_Of_Stock'),
        (101102103114, 'UK_Active', 123.99, True, 'In_Stock'),
        (101102103115, 'AL_Passive', 120.99, False, 'In_Stock'),
        (101102103116, 'FE_Active', 49.99, True, 'Out_Of_Stock'),
        (101102103117, 'WQ_Active', 32.99, True, 'Out_Of_Stock'),
        (101102103118, 'PO_Active', 23.99, True, 'Out_Of_Stock'),
        (101102103119, 'MO_Passive', 87.99, False, 'In_Stock'),
        (101102103120, 'LL_Passive', 20.99, True, 'Out_Of_Stock'),
        (101102103121, 'JK_Active', 23.99, True, 'In_Stock'),
        (101102103122, 'KK_Active', 65.99, False, 'Out_Of_Stock'),
        (101102103123, 'SE_Passive', 12.99, True, 'In_Stock'),
    ],"id bigint, name string, price double, is_active boolean, status string"
)

sample_data.write.format("delta").mode('overwrite').saveAsTable('test.bronze.PricingSamples')

for curr_env in curr_envs:
    print(f'Welcome to {curr_env}😊')