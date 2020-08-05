def get_zarr_id(zstore):
    """given a GC zarr location, return the dataset_id"""
    assert zstore[:10] == 'gs://cmip6'
    return zstore[11:-1].split('/')

def get_zarr_dict(zstore):
    """given a GC zarr location, return a dictionary of keywords"""
    zid = get_zarr_id(zstore)
    keys = ['activity_id','institution_id','source_id','experiment_id','member_id','table_id','variable_id','grid_label']
    values = list(zid)
    return dict(zip(keys,values))

def get_tracking_ids(zstore):
    """given a GC zarr location, fetch the associated dataset/netCDF tracking IDs"""
    from requests import session
    from zarr import open_consolidated
    from fsspec import get_mapper
    
    # request params
    client = session()
    base_url = 'http://hdl.handle.net/api/handles/'
    dset_id_query = '?type=IS_PART_OF'
    version_query = '?type=VERSION_NUMBER'

    # get primary tracking id
    netcdf_tracking_ids = open_consolidated(get_mapper(zstore)).attrs['tracking_id'].split('\n')
    file_tracking_id = netcdf_tracking_ids[0]
    
    version_ids = []
    dataset_ids = []
    
    # query for dataset_tracking_id
    dset_id_url = base_url + file_tracking_id[4:] + dset_id_query
    r = client.get(dset_id_url)
    r.raise_for_status()
    dataset_tracking_id = r.json()['values'][0]['data']['value']
    
    # query for version
    if ';' in dataset_tracking_id:
        # multiple dataset_ids erroneously reported
        dataset_tracking_id = "ambiguous"
    
    return dataset_tracking_id, netcdf_tracking_ids

def make_row(
    zstore, 
    status, 
    version, 
    severity=None, 
    issue_url=None, 
    dataset_tracking_id=None, 
    netcdf_tracking_ids=None
):
    """given a GC zarr location (and additional metadata), return a dictionary to be inserted via BigQuery"""
    
    # make initial dictionary
    d = get_zarr_dict(zstore)
    d["zstore"] = zstore
    d["status"] = status
    d["severity"] = severity
    d["issue_url"] = issue_url
    d["version"] = version
    
    # split up member ID
    if "-" in d["member_id"]:
        d["sub_experiment_id"] = d["member_id"].split("-")[0]
        d["variant_label"] = d["member_id"].split("-")[1]
    else:
        d["sub_experiment_id"] = None
        d["variant_label"] = d["member_id"]
        
    # fetch tracking IDs if not available
    if dataset_tracking_id and netcdf_tracking_ids:
        d["dataset_tracking_id"] = dataset_tracking_id
        d["netcdf_tracking_ids"] = netcdf_tracking_ids
    else:
        dataset_tracking_id, netcdf_tracking_ids = get_tracking_ids(zstore)
        d["dataset_tracking_id"] = dataset_tracking_id
        d["netcdf_tracking_ids"] = netcdf_tracking_ids
    
    return d
    
def make_rows(zstores, metadata):
    """given a list of GC zarr locations (and additional metadata in a dictionary),
       return a list of dictionaries to be inserted via BigQuery"""
    
    rows = []
    
    for zstore in zstores:
        d = make_row(zstore, 
                     metadata[zstore]["status"],
                     metadata[zstore]["version"],
                     metadata[zstore].get("severity"), 
                     metadata[zstore].get("issue_url"),
                     metadata[zstore].get("dataset_tracking_id"),
                     metadata[zstore].get("netcdf_tracking_ids")
                    )

        rows.append(d)
        
    return rows

def insert_rows(rows, table):
    """given a list of dictionaries representing rows in BigQuery, insert into CMIP6 table"""
    from google.cloud import bigquery
    
    row_keys = ['activity_id', 'institution_id', 'source_id', 'experiment_id', 'member_id', 'sub_experiment_id',
                'variant_label', 'table_id', 'variable_id', 'grid_label', 'zstore', 'dataset_tracking_id',
                'netcdf_tracking_ids', 'status', 'severity', 'issue_url', 'version']
    
    query = """
        INSERT `pangeo-181919.pangeo_cmip6.%s` %s
        VALUES %s""" % (table,
                        str(tuple(row_keys)).replace("'", ""), 
                        ",".join([str(tuple(row[key] for key in row_keys)) for row in rows]).replace("None", "NULL"))
    
    client = bigquery.Client()
    query_job = client.query(query)
    