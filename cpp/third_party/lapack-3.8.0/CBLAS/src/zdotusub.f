c     zdotusub.f
c
c     The program is a fortran wrapper for zdotu.
c     Witten by Keita Teranishi.  2/11/1998
c
      subroutine zdotusub(n,x,incx,y,incy,dotu)
c
      external zdotu
      double complex zdotu,dotu
      integer n,incx,incy
      double complex x(*),y(*)
c
      dotu=zdotu(n,x,incx,y,incy)
      return
      end
