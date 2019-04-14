c     cdotusub.f
c
c     The program is a fortran wrapper for cdotu.
c     Witten by Keita Teranishi.  2/11/1998
c
      subroutine cdotusub(n,x,incx,y,incy,dotu)
c
      external cdotu
      complex cdotu,dotu
      integer n,incx,incy
      complex x(*),y(*)
c
      dotu=cdotu(n,x,incx,y,incy)
      return
      end
