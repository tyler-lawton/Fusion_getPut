FROM python:3.10-alpine
ADD bin/getApp.py .
ADD bin/putApp.py .
ADD requirements.txt .
RUN pip install -r requirements.txt
CMD [ "python3", "getApp.py", "-v", "-d" ]