c     ddotsub.f
c
c     The program is a fortran wrapper for ddot.
c     Witten by Keita Teranishi.  2/11/1998
c
      subroutine ddotsub(n,x,incx,y,incy,dot)
c
      external ddot
      double precision ddot
      integer n,incx,incy
      double precision x(*),y(*),dot
c
      dot=ddot(n,x,incx,y,incy)
      return
      end
