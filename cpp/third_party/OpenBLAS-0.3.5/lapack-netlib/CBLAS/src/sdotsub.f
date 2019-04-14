c     sdotsub.f
c
c     The program is a fortran wrapper for sdot.
c     Witten by Keita Teranishi.  2/11/1998
c
      subroutine sdotsub(n,x,incx,y,incy,dot)
c
      external sdot
      real sdot
      integer n,incx,incy
      real x(*),y(*),dot
c
      dot=sdot(n,x,incx,y,incy)
      return
      end
