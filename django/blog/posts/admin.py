from django.contrib import admin
from .models import Post,Comment,Tag
# Register your models here.
@admin.register(Post)
class PostAdmin(admin.ModelAdmin): ##for more control over the admin page
    list_display = ['id','post_title','published_date']
    list_display_links = ['id','post_title']
    list_filter = ['published_date'] ## It will give us a filter
    search_fields = ['post_title']
#admin.site.register(Post, PostAdmin)
admin.site.register(Comment)
admin.site.register(Tag)