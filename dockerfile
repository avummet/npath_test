FROM python:3
MAINTAINER ananth vummethala <ananthvummethala@discover.com>

WORKDIR /app
ADD . /app
RUN pip install  -r requirements.txt
CMD [ "python", "transform_aster_npath.py" ]
