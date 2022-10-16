FROM python
WORKDIR /AbelianDatabaseMaintenance
COPY . /AbelianDatabaseMaintenance
RUN pip3 install -r requirements.txt
CMD ["python3","App.py"]

