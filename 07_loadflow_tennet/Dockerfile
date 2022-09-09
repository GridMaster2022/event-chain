FROM public.ecr.aws/lambda/python:3.8

# Copy function code
COPY *.py ${LAMBDA_TASK_ROOT}/
ADD sql ${LAMBDA_TASK_ROOT}/sql/
ADD networktools ${LAMBDA_TASK_ROOT}/networktools/
ADD data ${LAMBDA_TASK_ROOT}/data/

# Install the function's dependencies using requirements.txt
COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Set the CMD to the lambda handler
CMD [ "app.lambda_handler" ]