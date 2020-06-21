from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework.response import Response


class db_check(APIView):

    def get(self, request):
        res = {"check": 'positive'}
        print('API Working')
        return Response(res)


