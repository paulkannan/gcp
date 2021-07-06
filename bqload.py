import csv, os, sys
from util.gcsutils import GCSUtils
from google.cloud import storage
from datetime import datetime
from util.fileutils import FileUtils
from google.cloud import bigquery
import traceback
from util.context import Context


class BQ_Load:
    def execute(self):
        prefix = 'process-name'
        context = Context(prefix)
        context.logger.info("Started {0}".format(prefix))

        try:
            #Variable init
            _csv_gcs_bucket_name = context.settings.get("GCS_BUCKET_NAME")
            _google_project_id_bq = context.settings.get("GCS_PROJECT_ID_BQ")
            _csv_gcs_directory = context.settings.get("CSV_GCS_LANDING_PATH")
            _gcs_archived_path = context.settings.get("CSV_GCS_ARCHIVED")
            _gcs_failed_path = context.settings.get("CSV_GCS_FAILED")
            _bq_dataset_location = context.settings.get("DATASET_LOCATION")
            _src_and_target_details = context.settings.get("SRC_AND_TGT")

            client = bigquery.Client(_google_project_id_bq)
            storage_client = storage.Client()

            source_and_targets = _src_and_target_details.split(',')

            for src_and_tgt in source_and_targets:
                try:
                    sat = src_and_tgt.split('~')
                    source = sat[0].strip()
                    target = sat[1].strip()
                    print('Processing file for ', source)

                    #currdate = datetime.now().stfftime("%Y%m%d")

                    pattern = '{0}{1}_*.csv'.format(_csv_gcs_directory, source)
                    print('Bucket Search Pattern: '+pattern)
                    context.logger.info('Bucket Search Pattern: '+pattern)

                    fileList = GCSUtils.list_files(storage_client, _csv_gcs_bucket_name, pattern)
                    context.logger.info('No of files in GCS {0}'.format(len(fileList)))

                    #Step 1 : if list is empty, end the job
                    if len(fileList) == 0:
                        context.logger.info('No files to be processed for ', source)

                    #Step 2 : if List is not empty, Iterate over
                    else:
                        for file in fileList:
                            fileStr = str(file)
                            print("filename: ", fileStr)
                            csv_URL = 'gs: //' + _csv_gcs_bucket_name + '/' + fileStr
                            print ("filename : ", fileStr)
                            csv_URL = 'gs: //' + _csv_gcs_bucket_name + '/' + fileStr
                            print('csr_URL: ', csv_URL)
                            dbdetails = target.split('.')
                            dataset_id = dbdetails[0].strip()
                            TABLE_NAME = dbdetails[1].strip()

                            try:
                                print('{0} file is loading to BQ table {1}'.format(source, target))
                                context.logger.info('{0} file is loading to Bq {1}'.format(source,target))
                                dataset_ref = client.dataset(dataset_id)
                                job_config = bigquery.LoadJobConfig()
                                job_config.source_format = bigquery.SourceFormat.CSV
                                job_config.skip_leading_rows = bigquery.skip_leading_rows = 1
                                load_job = client.load_table_from_uri(
                                    csv_URL,
                                    dataset_ref.table(TABLE_NAME),
                                    location = _bq_dataset_location,
                                    job_config = job_config()
                                )
                                load_job.result()

                                #destination_table = client.get_table(TABLE_NAME)
                                #print("Loaded {} rows".format(destination_table.num_rows))

                                print("BQ bulk load completed to {0}, Status : {1}".format(target, load_job.state))
                                context.logger.info("BQ bulk load completed to {0}, Status : {1}".format(target, load_job.state))
                                print("End bulk load")

                                #Moving to success folder
                                try:
                                    GCSUtils.copy_blob(storage_client, _csv_gcs_bucket_name, fileStr, _csv_gcs_bucket_name, _gcs_archived_path+fileStr.split("/")[-1],context)
                                    GCSUtils.delete_blob(storage_client, _csv_gcs_bucket_name, fileStr, context)
                                except Exception as e:
                                    print('Error in ', e.__cause__)

                            except Exception as e:
                                print('Exception in ', e)
                                print(e.__cause__)
                                context.logger.error('Error in bulk load for : {0}'.format(sys.exec_info()[1]))
                                context.logger.error('Error in bulk load for : {0}'.format(traceback.format_exc()))

                                #Move to failed folder
                                try:
                                    GCSUtils.copy_blob(storage_client, _csv_gcs_bucket_name, fileStr, _csv_gcs_bucket_name, _gcs_archived_path + fileStr.split("/")[-1], context)
                                    GCSUtils.delete_blob(storage_client, _csv_gcs_bucket_name, fileStr, context)
                                except Exception as e:
                                    print('Error in ', e.__cause__)
                except Exception as e:
                    print('Error ', e)

        except Exception as e:
            print('Exception in ', e)
            print(e.__cause__)
            context.logger.error('Error in bulk load for : {0}'.format(sys.exec_info()[1]))
            context.logger.error('Error in bulk load for : {0}'.format(traceback.format_exc()))

if __name__ == '__main__':
    a = BQ_Load
    a.execute()

