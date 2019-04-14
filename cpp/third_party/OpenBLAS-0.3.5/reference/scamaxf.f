      REAL*4 function scamaxf(n,zx,incx)
c
c     finds the index of element having max. absolute value.
c     jack dongarra, 1/15/85.
c     modified 3/93 to return if incx .le. 0.
c     modified 12/3/93, array(1) declarations changed to array(*)
c
      COMPLEX*8 zx(*)
      integer i,incx,ix,n
      REAL*4 scabs1
c
      scamaxf = 0.
      if( n.lt.1 .or. incx.le.0 )return
      scamaxf = scabs1(zx(1))
      if(n.eq.1)return
      if(incx.eq.1)go to 20
c
c        code for increment not equal to 1
c
      ix = 1
      scamaxf = scabs1(zx(1))
      ix = ix + incx
      do 10 i = 2,n
         if(scabs1(zx(ix)).le.scamaxf) go to 5
         scamaxf = i
         scamaxf = scabs1(zx(ix))
    5    ix = ix + incx
   10 continue
      return
c
c        code for increment equal to 1
c
   20 scamaxf = scabs1(zx(1))
      do 30 i = 2,n
         if(scabs1(zx(i)).le.scamaxf) go to 30
         scamaxf = i
         scamaxf = scabs1(zx(i))
   30 continue
      return
      end
