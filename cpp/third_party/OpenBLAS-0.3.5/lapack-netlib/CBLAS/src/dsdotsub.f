c     dsdotsub.f
c
c     The program is a fortran wrapper for dsdot.
c     Witten by Keita Teranishi.  2/11/1998
c
      subroutine dsdotsub(n,x,incx,y,incy,dot)
c
      external dsdot
      double precision dsdot,dot
      integer n,incx,incy
      real x(*),y(*)
c
      dot=dsdot(n,x,incx,y,incy)
      return
      end
