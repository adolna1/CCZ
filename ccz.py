from azure.identity import ClientSecretCredential 
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.datafactory.models import *
from azure.identity import AzureCliCredential
from azure.storage.blob import BlockBlobService
from azure.storage.blob import ContentSettings
from datetime import datetime, timedelta
import time, random

def print_item(group):
    """Print an Azure object instance."""
    print("\tName: {}".format(group.name))
    print("\tId: {}".format(group.id))
    if hasattr(group, 'location'):
        print("\tLocation: {}".format(group.location))
    if hasattr(group, 'tags'):
        print("\tTags: {}".format(group.tags))
    if hasattr(group, 'properties'):
        print_properties(group.properties)

def print_properties(props):
    """Print a ResourceGroup properties instance."""
    if props and hasattr(props, 'provisioning_state') and props.provisioning_state:
        print("\tProperties:")
        print("\t\tProvisioning State: {}".format(props.provisioning_state))
    print("\n\n")

def print_activity_run_details(activity_run):
    """Print activity run details."""
    print("\n\tActivity run details\n")
    print("\tActivity run status: {}".format(activity_run.status))
    if activity_run.status == 'Succeeded':
        print("\tNumber of bytes read: {}".format(activity_run.output['dataRead']))
        print("\tNumber of bytes written: {}".format(activity_run.output['dataWritten']))
        print("\tCopy duration: {}".format(activity_run.output['copyDuration']))
    else:
        print("\tErrors: {}".format(activity_run.error['message']))


def main():

    subscription_id = 'ede8b2f3-d553-416e-a398-131452cfb73c'

    rg_name = 'rg_adolna'

    df_name = 'adolna'

    credentials = AzureCliCredential()
    resource_client = ResourceManagementClient(credentials, subscription_id)
    adf_client = DataFactoryManagementClient(credentials, subscription_id)

    rg_params = {'location':'centralus'}
    df_params = {'location':'centralus'}
 
    resource_client.resource_groups.create_or_update(rg_name, rg_params)

    df_resource = Factory(location='westus')
    df = adf_client.factories.create_or_update(rg_name, df_name, df_resource)
    print_item(df)
    while df.provisioning_state != 'Succeeded':
        df = adf_client.factories.get(rg_name, df_name)
        time.sleep(1)

    ls_name = 'storageLinkedService001'

    storage_client = StorageManagementClient(credentials, subscription_id)
    STORAGE_ACCOUNT_NAME = f"adolna{random.randint(1,100000):05}"

    poller = storage_client.storage_accounts.begin_create(rg_name, STORAGE_ACCOUNT_NAME,
    {
        "location" : 'centralus',
        "kind": "StorageV2",
        "sku": {"name": "Standard_LRS"}
    }
    )
    account_result = poller.result()
    
    keys = storage_client.storage_accounts.list_keys(rg_name, STORAGE_ACCOUNT_NAME)

    storage_string = f"DefaultEndpointsProtocol=https;AccountName={STORAGE_ACCOUNT_NAME};AccountKey={keys.keys[0].value}"

    ls_azure_storage = LinkedServiceResource(properties=AzureStorageLinkedService(connection_string=storage_string)) 
    ls = adf_client.linked_services.create_or_update(rg_name, df_name, ls_name, ls_azure_storage)


    block_blob_service = BlockBlobService(account_name=STORAGE_ACCOUNT_NAME, account_key=keys.keys[0].value)
    block_blob_service.create_container('adolnablob')
    # time.sleep(5)
    block_blob_service.create_blob_from_path('adolnablob', 'inputdata/ccz.txt', 'ccz.txt', content_settings=ContentSettings(content_type='text/plain'))


    ds_name = 'ds_in'
    ds_ls = LinkedServiceReference(reference_name=ls_name)
    blob_path = 'adolnablob/inputdata'
    blob_filename = 'ccz.txt'
    ds_azure_blob = DatasetResource(properties=AzureBlobDataset(
        linked_service_name=ds_ls, folder_path=blob_path, file_name=blob_filename))
    ds = adf_client.datasets.create_or_update(
        rg_name, df_name, ds_name, ds_azure_blob)
    print_item(ds)

    dsOut_name = 'ds_out'
    output_blobpath = 'adolnablob/outputdata'
    dsOut_azure_blob = DatasetResource(properties=AzureBlobDataset(linked_service_name=ds_ls, folder_path=output_blobpath))
    dsOut = adf_client.datasets.create_or_update(
        rg_name, df_name, dsOut_name, dsOut_azure_blob)
    print_item(dsOut)

    act_name = 'copyBlobtoBlob'
    blob_source = BlobSource()
    blob_sink = BlobSink()
    dsin_ref = DatasetReference(reference_name=ds_name)
    dsOut_ref = DatasetReference(reference_name=dsOut_name)
    copy_activity = CopyActivity(name=act_name, inputs=[dsin_ref], outputs=[
                                 dsOut_ref], source=blob_source, sink=blob_sink)

    p_name = 'copyPipeline'
    params_for_pipeline = {}
    p_obj = PipelineResource(
        activities=[copy_activity], parameters=params_for_pipeline)
    p = adf_client.pipelines.create_or_update(rg_name, df_name, p_name, p_obj)
    print_item(p)

    run_response = adf_client.pipelines.create_run(rg_name, df_name, p_name, parameters={})

    time.sleep(30)
    pipeline_run = adf_client.pipeline_runs.get(
        rg_name, df_name, run_response.run_id)
    print("\n\tPipeline run status: {}".format(pipeline_run.status))
    filter_params = RunFilterParameters(
        last_updated_after=datetime.now() - timedelta(1), last_updated_before=datetime.now() + timedelta(1))
    query_response = adf_client.activity_runs.query_by_pipeline_run(
        rg_name, df_name, pipeline_run.run_id, filter_params)
    print_activity_run_details(query_response.value[0])


main()