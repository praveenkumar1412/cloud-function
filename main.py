from google.cloud import run_v2
 
def run_job(f, b):
    override_spec = {
        "container_overrides": [
            {
                "args": [f"--file={f}", f"--bucket={b}"]
            }
        ]
    }
    
    print(f"Starting jobs with file: {f} & bucket: {b}")
    
    try:
        client = run_v2.JobsClient()

        job_request_1 = run_v2.RunJobRequest(
            name="projects/project-4-workndemos/locations/us-central1/jobs/pk-pipeline",
            overrides=override_spec
        )
        operation_1 = client.run_job(request=job_request_1)
        print("Started job")

       

    except Exception as e:
        print(f"Error running job: {e}")

def process(event, context):
    try:
        file_name = event['name']
        bucket_name = event['bucket']
        
        print(f"Processing event for file: {file_name} in bucket: {bucket_name}")
        
        run_job(file_name, bucket_name)
    
    except Exception as e:
        print(f"Error processing event: {e}")
