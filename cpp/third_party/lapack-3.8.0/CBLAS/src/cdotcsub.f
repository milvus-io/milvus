c     cdotcsub.f
c
c     The program is a fortran wrapper for cdotc.
c     Witten by Keita Teranishi.  2/11/1998
c
      subroutine cdotcsub(n,x,incx,y,incy,dotc)
c
      external cdotc
      complex cdotc,dotc
      integer n,incx,incy
      complex x(*),y(*)
c
      dotc=cdotc(n,x,incx,y,incy)
      return
      end
