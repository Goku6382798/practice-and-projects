def myFilter(c, f):
   c_f = []
   for e in c:
       if(f(e)):
           c_f.append(e)
   return c_f

def myMap(c, f):
   c_f = []
   for e in c:
       c_f.append(f(e))
   return c_f

def myReduce(c, f):
   t = c[0]
   for e in c[1:]:
       t = f(t, e)
   return t

def myReduceByKey(p, f):
   p_f = {}
   for e in p:
       if(e[0] in p_f):
           p_f[e[0]] = f(p_f[e[0]], e[1])
       else:
           p_f[e[0]] = e[1]
   return list(p_f.items())