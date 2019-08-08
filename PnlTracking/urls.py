from django.contrib import admin
from django.urls import path, re_path, include
from PnlTracking import views


urlpatterns = [
    re_path(r'^index/', views.index),
    re_path(r'^show/', views.show),
    re_path(r'^test/', views.test),
    re_path(r'^testIndex/', views.testIndex),
    re_path(r'^ajax_pnl/', views.ajax_pnl, name='ajax-pnl'),
]
