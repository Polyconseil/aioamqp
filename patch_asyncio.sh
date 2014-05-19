#!/bin/sh

# asyncio fails weirdly when PYTHONASYNCIODEBUG is set
# see http://bugs.python.org/issue21209 for more details

echo "--- lib/python3.3/site-packages/asyncio/tasks.py 2014-05-16 17:11:42.000000000 +0200
+++ lib/python3.3/site-packages/asyncio/tasks.py 2014-05-16 17:11:58.000000000 +0200
@@ -50,7 +50,7 @@
         return next(self.gen)

     def send(self, value):
-        return self.gen.send(value)
+        return self.gen.send((value,))

     def throw(self, exc):
         return self.gen.throw(exc)
" | patch -p0 --forward --dir=$VIRTUAL_ENV || true
