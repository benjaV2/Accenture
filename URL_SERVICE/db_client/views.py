from rest_framework.views import APIView
from rest_framework.response import Response
from pymongo import MongoClient, errors


class db_check(APIView):

    def get(self, request):
        res = {"check": 'positive'}
        print('API Working')
        return Response(res)


class db_insert(APIView):

    def post(self, request):
        client = MongoClient('10.0.130.73', 27017)
        db = client["URL"]
        table = db.U
        names = request.data['names']
        payload = [{'name': name} for name in names]
        try:
            dup = []
            rs = table.insert_many(payload, ordered=False)
        except errors.BulkWriteError as e:
            for error in e.details['writeErrors']:
                if error['code'] == 11000:
                    dup.append(error["op"]["name"])
        new = list(set(names) - set(dup))
        print(request.data)
        print(new)
        return Response(200)


