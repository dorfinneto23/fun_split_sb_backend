import azure.functions as func
import logging

app = func.FunctionApp()

@app.service_bus_queue_trigger(arg_name="azservicebus", queue_name="split",
                               connection="medicalanalysis_SERVICEBUS") 
def sb_split_process(azservicebus: func.ServiceBusMessage):
    logging.info('Python ServiceBus Queue trigger processed a message: %s',
                azservicebus.get_body().decode('utf-8'))
