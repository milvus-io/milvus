      REAL*8 function dzaminf(n,zx,incx)
c
c     finds the index of element having min. absolute value.
c     jack dongarra, 1/15/85.
c     modified 3/93 to return if incx .le. 0.
c     modified 12/3/93, array(1) declarations changed to array(*)
c
      COMPLEX*16 zx(*)
      integer i,incx,ix,n
      double precision dcabs1
c
      dzaminf = 0.
      if( n.lt.1 .or. incx.le.0 )return
      dzaminf = dcabs1(zx(1))
      if(n.eq.1)return
      if(incx.eq.1)go to 20
c
c        code for increment not equal to 1
c
      ix = 1
      dzaminf = dcabs1(zx(1))
      ix = ix + incx
      do 10 i = 2,n
         if(dcabs1(zx(ix)).ge.dzaminf) go to 5
         dzaminf = dcabs1(zx(ix))
    5    ix = ix + incx
   10 continue
      return
c
c        code for increment equal to 1
c
   20 dzaminf = dcabs1(zx(1))
      do 30 i = 2,n
         if(dcabs1(zx(i)).ge.dzaminf) go to 30
         dzaminf = dcabs1(zx(i))
   30 continue
      return
      end
