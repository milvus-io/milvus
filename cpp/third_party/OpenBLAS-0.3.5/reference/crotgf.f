      subroutine crotgf(ca,cb,c,s)
      complex ca,cb,s
      real c
      real norm,scale
      complex alpha
      if (cabs(ca) .ne. 0.) go to 10
         c = 0.
         s = (1.,0.)
         ca = cb
         go to 20
   10 continue
         scale = cabs(ca) + cabs(cb)
         norm = scale * sqrt((cabs(ca/scale))**2 + (cabs(cb/scale))**2)
         alpha = ca /cabs(ca)
         c = cabs(ca) / norm
         s = alpha * conjg(cb) / norm
         ca = alpha * norm
   20 continue
      return
      end
