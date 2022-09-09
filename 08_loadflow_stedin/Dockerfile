FROM public.ecr.aws/lambda/python:3.8

# Copy function code
COPY *.py ${LAMBDA_TASK_ROOT}/
ADD sql ${LAMBDA_TASK_ROOT}/sql/
ADD networktools ${LAMBDA_TASK_ROOT}/networktools/

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

CMD [ "app.lambda_handler" ]