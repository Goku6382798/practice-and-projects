from django.shortcuts import render
from django.http import HttpResponseRedirect
from django.urls import reverse
from .models import Post, Tag
from django.shortcuts import get_object_or_404
from django.core.paginator import Paginator
from .forms import CommentForm
from django.urls import reverse
from django.db.models import Q

posts = [
    {
        "id":1,
        "title":'Let\'s explore my life',
        "content":'Looking for a job currently in data engineerig.'
    },
    {
        "id":2,
        "title":'Where I am',
        "content":'I am currently in varanasi.'
    },
    {
        "id":3,
        "title":'What I love in food',
        "content":'It completely depands over situation like if I am in Varanasi I love samosa chola the most. When in delhi love beer and paneer with kala namak and kaali mirach.'
    },
    {
        "id":4,
        "title":'Why do I drink',
        "content":'It helps me reducing stress and makes me feel like a young person.'
    }
]

def home(request):
    if request.user.is_authenticated:
        all_posts = Post.objects.all().order_by('-id')
        paginator = Paginator(all_posts,4,orphans=2)
        page_number = request.GET.get('p',1)
        page_obj = paginator.get_page(page_number)
        #print(page_obj)
        return render(request,'posts/index.html',{'posts':page_obj})
    else:
        return HttpResponseRedirect('/accounts/login')

def post(request, id):
    post = get_object_or_404(Post,id=id)
    if request.method == 'POST':
        form = CommentForm(request.POST)
        if form.is_valid():
            comment = form.save(commit=False)
            comment.post = post
            comment.user = request.user
            comment.save()
            posturl = reverse('post',args=[id])
            return HttpResponseRedirect(posturl)
    
    form = CommentForm()
    return render(request,"posts/post.html",{'post_dict':post,'form':form,'comments':post.comment_set.all()})
def google(request, id):
    url = reverse("post",args=[id])
    return HttpResponseRedirect(url)

def global1(request):
    return render(request,'global1.html')

def Tags(request,id):
    tag = Tag.objects.get(id=id)
    return render(request,'posts/tags.html',{'tags':tag.post_set.all()})

def search(request):
    query = request.GET.get('query','')
    page_number = request.GET.get('p',1)
    posts = Post.objects.filter(Q(post_title__icontains=query) | Q(post_content__icontains=query)).order_by('-id')
    paginator = Paginator(posts,4)
    page_obj = paginator.get_page(page_number)
    return render(request,'posts/search.html',{'posts':page_obj,'query':query,'total':posts.count()})  ##as it will not pass the query in the next page thats why we are using it