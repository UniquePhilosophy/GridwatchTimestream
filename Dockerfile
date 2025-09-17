FROM public.ecr.aws/lambda/python:3.10

COPY requirements.txt ./
RUN pip install --upgrade pip
RUN pip install -r requirements.txt --no-cache-dir

COPY . /var/task

CMD ["gridwatch_lambda.lambda_handler"]
