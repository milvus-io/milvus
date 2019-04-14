c     isamaxsub.f
c
c     The program is a fortran wrapper for isamax.
c     Witten by Keita Teranishi.  2/11/1998
c
      subroutine isamaxsub(n,x,incx,iamax)
c
      external isamax
      integer  isamax,iamax
      integer n,incx
      real x(*)
c
      iamax=isamax(n,x,incx)
      return
      end
