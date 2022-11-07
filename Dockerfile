FROM python:3.10-alpine
ADD bin/get_fusion_app.py .
#ADD bin/put_fusion_app.py .
ADD requirements.txt .
RUN pip install -r requirements.txt
CMD [ "python3", "get_fusion_app.py", "-v", "-d" ]