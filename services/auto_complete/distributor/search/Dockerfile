FROM python:3 
ENV PYTHONUNBUFFERED 1 
WORKDIR /search_services
COPY . /search_services
RUN pip install -r requirements.txt
CMD ["python","main.py"]