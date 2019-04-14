/**********************************Zero Vectors**************************************************/

.macro ZERO_CVEC_8x4
    vzero  %v16  
    vzero  %v17   
    vzero  %v18   
    vzero  %v19   
    vzero  %v20   
    vzero  %v21   
    vzero  %v22   
    vzero  %v23   
    vzero  %v24   
    vzero  %v25   
    vzero  %v26   
    vzero  %v27   
    vzero  %v28   
    vzero  %v29   
    vzero  %v30   
    vzero  %v31 
.endm


.macro ZERO_CVEC_8x2
    vzero  %v16  
    vzero  %v17   
    vzero  %v18   
    vzero  %v19   
    vzero  %v20   
    vzero  %v21   
    vzero  %v22   
    vzero  %v23   
     
.endm

.macro ZERO_CVEC_8x1
    vzero  %v16  
    vzero  %v17
    vzero  %v18  
    vzero  %v19     
.endm

.macro ZERO_CVEC_4x4
    vzero  %v16  
    vzero  %v17    
    vzero  %v20   
    vzero  %v21    
    vzero  %v24   
    vzero  %v25     
    vzero  %v28   
    vzero  %v29   
.endm

.macro ZERO_CVEC_4x2
    vzero  %v16  
    vzero  %v17     
    vzero  %v20   
    vzero  %v21    
     
.endm

.macro ZERO_CVEC_4x1
     lzer %f1
     lzer %f2 
     lzer %f3
     lzer %f4 
.endm

.macro ZERO_CVEC_2x4
    vzero  %v16  
    vzero  %v17     
    vzero  %v20   
    vzero  %v21    
     
.endm

.macro ZERO_CVEC_2x2
    vzero  %v16       
    vzero  %v20      
     
.endm

.macro ZERO_CVEC_2x1
     lzer %f1
     lzer %f2   
.endm

.macro ZERO_CVEC_1x4
     lzer %f1
     lzer %f2 
     lzer %f3
     lzer %f4      
.endm

.macro ZERO_CVEC_1x2
     lzer %f1
     lzer %f2      
.endm

.macro ZERO_CVEC_1x1
    lzer %f1       
.endm


/***********************************Helper Calculations*************************************/
#define unit_size 4
#define DISP(ind,stride,disp) (ind*stride+disp)
#define DISP8(ind,disp) (ind*unit_size*8+disp)
#define DISP4(ind,disp) (ind*unit_size*4+disp)
#define DISP2(ind,disp) (ind*unit_size*2+disp)
#define DISP1(ind,disp) (ind*unit_size+disp)
#define N8  (8*unit_size)
#define N4  (4*unit_size)
#define N2  (2*unit_size)
#define N1  (1*unit_size)

.macro Calculate_8x4_I  PTR_A_REG,PTR_B_REG,Index,IsLast
        vlm  %v1,%v2, DISP8(\Index , 0)(\PTR_A_REG)  
        vmrhf %v3,%v1,%v1  
        vmrhf %v5,%v2,%v2 
        vmrlf  %v4,%v1,%v1
        vmrlf  %v6,%v2,%v2 
        vldeb  %v3, %v3
        vldeb  %v4, %v4    
        vldeb  %v5, %v5   
        vlrepf %v7, DISP4(\Index ,0)(\PTR_B_REG)  
        vlrepf %v1, DISP4(\Index ,4)(\PTR_B_REG) 
        vldeb  %v6, %v6 
        vldeb  %v7, %v7
        vldeb  %v1, %v1    
 
  
        vfmadb    %v16,%v3,%v7,%v16   
        vfmadb    %v17,%v4,%v7,%v17   
        vfmadb    %v18,%v5,%v7,%v18  
        vfmadb    %v19,%v6,%v7,%v19     
        vfmadb    %v20,%v3,%v1,%v20   
        vfmadb    %v21,%v4,%v1,%v21 
        vfmadb    %v22,%v5,%v1,%v22  
        vfmadb    %v23,%v6,%v1,%v23  
        vlrepf %v2, DISP4(\Index ,8)(\PTR_B_REG)  
        vlrepf %v7, DISP4(\Index ,12)(\PTR_B_REG) 
        vldeb  %v2, %v2
        vldeb  %v7, %v7 
   .if \IsLast==1         
        la \PTR_A_REG, DISP8(\Index ,N8)(\PTR_A_REG)   
   .endif    
        vfmadb    %v24,%v3,%v2,%v24   
        vfmadb    %v25,%v4,%v2,%v25   
        vfmadb    %v26,%v5,%v2,%v26  
        vfmadb    %v27,%v6,%v2,%v27        
        vfmadb    %v28,%v3,%v7,%v28   
        vfmadb    %v29,%v4,%v7,%v29   
        vfmadb    %v30,%v5,%v7,%v30  
        vfmadb    %v31,%v6,%v7,%v31
    .if \IsLast==1      
        la \PTR_B_REG, DISP4(\Index ,N4)(\PTR_B_REG)
    .endif    
.endm

.macro Calculate_8x2_I  PTR_A_REG,PTR_B_REG,Index,IsLast
        vlm  %v1,%v2, DISP8(\Index , 0)(\PTR_A_REG)  
        vmrhf %v3,%v1,%v1  
        vmrhf %v5,%v2,%v2 
        vmrlf  %v4,%v1,%v1
        vmrlf  %v6,%v2,%v2 
        vldeb  %v3, %v3
        vldeb  %v4, %v4
        vldeb  %v5, %v5   
        vlrepf %v7, DISP2(\Index ,0)(\PTR_B_REG)  
        vlrepf %v1, DISP2(\Index ,4)(\PTR_B_REG) 
        vldeb  %v6, %v6
        vldeb  %v7, %v7
        vldeb  %v1, %v1    
        vfmadb    %v16,%v3,%v7,%v16   
        vfmadb    %v17,%v4,%v7,%v17   
        vfmadb    %v18,%v5,%v7,%v18  
        vfmadb    %v19,%v6,%v7,%v19     
        vfmadb    %v20,%v3,%v1,%v20   
        vfmadb    %v21,%v4,%v1,%v21   
    .if \IsLast==1             
        la \PTR_A_REG, DISP8(\Index ,N8)(\PTR_A_REG)  
    .endif         
        vfmadb    %v22,%v5,%v1,%v22  
        vfmadb    %v23,%v6,%v1,%v23  
    .if \IsLast==1                
        la \PTR_B_REG, DISP2(\Index ,N2)(\PTR_B_REG)
    .endif         
.endm

.macro Calculate_8x1_I  PTR_A_REG,PTR_B_REG,Index,IsLast
        vlm  %v1,%v2, DISP8(\Index , 0)(\PTR_A_REG)  
        vmrhf %v3,%v1,%v1  
        vmrhf %v5,%v2,%v2 
        vmrlf  %v4,%v1,%v1
        vmrlf  %v6,%v2,%v2 
        vldeb  %v3, %v3
        vldeb  %v4, %v4
        vldeb  %v5, %v5   
        vlrepf %v7, DISP1(\Index ,0)(\PTR_B_REG)   
        vldeb  %v6, %v6
        vldeb  %v7, %v7
        vfmadb    %v16,%v3,%v7,%v16      
    .if \IsLast==1            
        la \PTR_B_REG, DISP1(\Index ,N1)(\PTR_B_REG) 
    .endif    
 
        vfmadb    %v17,%v4,%v7,%v17   
        vfmadb    %v18,%v5,%v7,%v18  
        vfmadb    %v19,%v6,%v7,%v19      
    .if \IsLast==1    
        la \PTR_A_REG, DISP8(\Index ,N8)(\PTR_A_REG)
    .endif      
.endm

.macro Calculate_4x4_I  PTR_A_REG,PTR_B_REG,Index,IsLast
        vl  %v5, DISP4(\Index , 0)(\PTR_A_REG) 
        vlrepf %v7, DISP4(\Index ,0)(\PTR_B_REG)  
        vlrepf %v1, DISP4(\Index ,4)(\PTR_B_REG) 
        vmrhf %v2,%v5,%v5  
        vmrlf  %v3,%v5,%v5
        vldeb  %v2, %v2
        vldeb  %v3, %v3 
        vldeb  %v7, %v7
        vldeb  %v1, %v1              
        vfmadb    %v16,%v2,%v7,%v16   
        vfmadb    %v17,%v3,%v7,%v17       
        vfmadb    %v20,%v2,%v1,%v20   
        vfmadb    %v21,%v3,%v1,%v21 
        vlrepf %v7, DISP4(\Index ,8)(\PTR_B_REG)      
        vlrepf %v1, DISP4(\Index ,12)(\PTR_B_REG)  
        vldeb  %v7, %v7
        vldeb  %v1, %v1
    .if \IsLast==1           
        la \PTR_A_REG, DISP4(\Index ,N4)(\PTR_A_REG)
    .endif           
        vfmadb    %v24,%v2,%v7,%v24   
        vfmadb    %v25,%v3,%v7,%v25  
        vfmadb    %v28,%v2,%v1,%v28   
        vfmadb    %v29,%v3,%v1,%v29 
    .if \IsLast==1               
        la \PTR_B_REG, DISP4(\Index ,N4)(\PTR_B_REG) 
    .endif      
.endm

.macro Calculate_4x2_I  PTR_A_REG,PTR_B_REG,Index,IsLast
        vl  %v5, DISP4(\Index , 0)(\PTR_A_REG) 
        vlrepf %v7, DISP2(\Index ,0)(\PTR_B_REG)  
        vlrepf %v1, DISP2(\Index ,4)(\PTR_B_REG) 
        vmrhf %v2,%v5,%v5  
        vmrlf  %v3,%v5,%v5
        vldeb  %v2, %v2
        vldeb  %v3, %v3   
        vldeb  %v7, %v7   
        vldeb  %v1, %v1    
        vfmadb    %v16,%v2,%v7,%v16   
        vfmadb    %v17,%v3,%v7,%v17 
    .if \IsLast==1    
        la \PTR_B_REG, DISP2(\Index ,N2)(\PTR_B_REG)  
    .endif          
        vfmadb    %v20,%v2,%v1,%v20   
        vfmadb    %v21,%v3,%v1,%v21  
    .if \IsLast==1       
        la \PTR_A_REG, DISP4(\Index ,N4)(\PTR_A_REG)   
    .endif      
.endm

.macro Calculate_4x1_I  PTR_A_REG,PTR_B_REG,Index,IsLast
        le %f5,DISP1(\Index ,0)(\PTR_B_REG)
        maeb %f1,%f5,DISP4(\Index ,0)(\PTR_A_REG) 
        maeb %f2,%f5,DISP4(\Index ,4)(\PTR_A_REG)     
    .if \IsLast==1                 
        la \PTR_B_REG, DISP1(\Index ,N1)(\PTR_B_REG)  
    .endif            
        maeb %f3,%f5,DISP4(\Index ,8)(\PTR_A_REG)  
        maeb %f4,%f5,DISP4(\Index ,12)(\PTR_A_REG)    
    .if \IsLast==1          
        la \PTR_A_REG, DISP4(\Index ,N4)(\PTR_A_REG)
    .endif         
.endm

.macro Calculate_2x2_I  PTR_A_REG,PTR_B_REG,Index,IsLast
        vlrepf %v7,  DISP2(\Index ,0)(\PTR_B_REG)  
        vlrepf %v1,  DISP2(\Index ,4)(\PTR_B_REG)   
        vlef	%v2, DISP2(\Index ,0)(\PTR_A_REG) ,0
        vlef	%v2, DISP2(\Index ,4)(\PTR_A_REG) ,2    
        vldeb  %v7, %v7
        vldeb	%v2,%v2  
        vldeb  %v1, %v1 

        vfmadb    %v16,%v2,%v7,%v16    
    .if \IsLast==1           
        la \PTR_A_REG, DISP2(\Index ,N2)(\PTR_A_REG)  
    .endif         
        vfmadb    %v20,%v2,%v1,%v20  
    .if \IsLast==1          
        la \PTR_B_REG, DISP2(\Index ,N2)(\PTR_B_REG)
    .endif        
.endm
 
 

.macro Calculate_2x1_I  PTR_A_REG,PTR_B_REG,Index,IsLast
         
        le %f3,DISP1(\Index ,0)(\PTR_B_REG)
        maeb %f1,%f3,DISP2(\Index ,0)(\PTR_A_REG)
    .if \IsLast==1              
        la \PTR_B_REG, DISP1(\Index ,N1)(\PTR_B_REG)
    .endif      
        maeb %f2, %f3,DISP2(\Index ,4)(\PTR_A_REG)
    .if \IsLast==1     
        la \PTR_A_REG, DISP2(\Index ,N2)(\PTR_A_REG)
    .endif         
.endm

.macro  Calculate_1x1_I  PTR_A_REG,PTR_B_REG,Index,IsLast
     le %f2,DISP1(\Index ,0)(\PTR_A_REG) /**a*/
    .if \IsLast==1  
      la  \PTR_A_REG,DISP1(\Index ,N1)(\PTR_A_REG)
    .endif 
     maeb %f1,%f2,DISP1(\Index ,0)(\PTR_B_REG)
    .if \IsLast==1       
      la  \PTR_B_REG,DISP1(\Index ,N1)(\PTR_B_REG) 
    .endif    
.endm

.macro CALC_8x4  PTR_A_REG,PTR_B_REG
   Calculate_8x4_I  \PTR_A_REG,\PTR_B_REG,0,1
.endm

.macro CALC_8x4_4  PTR_A_REG,PTR_B_REG 
   Calculate_8x4_I  \PTR_A_REG,\PTR_B_REG,0,0
   Calculate_8x4_I  \PTR_A_REG,\PTR_B_REG,1,0
   Calculate_8x4_I  \PTR_A_REG,\PTR_B_REG,2,0
   Calculate_8x4_I  \PTR_A_REG,\PTR_B_REG,3,1 
.endm

.macro CALC_8x2  PTR_A_REG,PTR_B_REG
   Calculate_8x2_I  \PTR_A_REG,\PTR_B_REG,0,1
.endm

.macro CALC_8x2_4  PTR_A_REG,PTR_B_REG 
   Calculate_8x2_I  \PTR_A_REG,\PTR_B_REG,0,0
   Calculate_8x2_I  \PTR_A_REG,\PTR_B_REG,1,0
   Calculate_8x2_I  \PTR_A_REG,\PTR_B_REG,2,0
   Calculate_8x2_I  \PTR_A_REG,\PTR_B_REG,3,1 
.endm

.macro CALC_8x1  PTR_A_REG,PTR_B_REG
   Calculate_8x1_I  \PTR_A_REG,\PTR_B_REG,0,1
.endm

.macro CALC_8x1_4  PTR_A_REG,PTR_B_REG 
   Calculate_8x1_I  \PTR_A_REG,\PTR_B_REG,0,0
   Calculate_8x1_I  \PTR_A_REG,\PTR_B_REG,1,0
   Calculate_8x1_I  \PTR_A_REG,\PTR_B_REG,2,0
   Calculate_8x1_I  \PTR_A_REG,\PTR_B_REG,3,1 
.endm

.macro CALC_4x4  PTR_A_REG,PTR_B_REG
   Calculate_4x4_I  \PTR_A_REG,\PTR_B_REG,0,1
.endm

.macro CALC_4x4_4  PTR_A_REG,PTR_B_REG 
   Calculate_4x4_I  \PTR_A_REG,\PTR_B_REG,0,0
   Calculate_4x4_I  \PTR_A_REG,\PTR_B_REG,1,0
   Calculate_4x4_I  \PTR_A_REG,\PTR_B_REG,2,0
   Calculate_4x4_I  \PTR_A_REG,\PTR_B_REG,3,1 
.endm

.macro CALC_4x2  PTR_A_REG,PTR_B_REG
   Calculate_4x2_I  \PTR_A_REG,\PTR_B_REG,0,1
.endm

.macro CALC_4x2_4  PTR_A_REG,PTR_B_REG 
   Calculate_4x2_I  \PTR_A_REG,\PTR_B_REG,0,0
   Calculate_4x2_I  \PTR_A_REG,\PTR_B_REG,1,0
   Calculate_4x2_I  \PTR_A_REG,\PTR_B_REG,2,0
   Calculate_4x2_I  \PTR_A_REG,\PTR_B_REG,3,1 
.endm

.macro CALC_4x1  PTR_A_REG,PTR_B_REG
   Calculate_4x1_I  \PTR_A_REG,\PTR_B_REG,0,1
.endm

.macro CALC_4x1_4  PTR_A_REG,PTR_B_REG 
   Calculate_4x1_I  \PTR_A_REG,\PTR_B_REG,0,0
   Calculate_4x1_I  \PTR_A_REG,\PTR_B_REG,1,0
   Calculate_4x1_I  \PTR_A_REG,\PTR_B_REG,2,0
   Calculate_4x1_I  \PTR_A_REG,\PTR_B_REG,3,1 
.endm

.macro CALC_2x4  PTR_A_REG,PTR_B_REG
   Calculate_4x2_I  \PTR_B_REG,\PTR_A_REG,0,1
.endm

.macro CALC_2x4_4  PTR_A_REG,PTR_B_REG 
   Calculate_4x2_I  \PTR_B_REG,\PTR_A_REG,0,0
   Calculate_4x2_I  \PTR_B_REG,\PTR_A_REG,1,0
   Calculate_4x2_I  \PTR_B_REG,\PTR_A_REG,2,0
   Calculate_4x2_I  \PTR_B_REG,\PTR_A_REG,3,1 
.endm

.macro CALC_2x2  PTR_A_REG,PTR_B_REG
   Calculate_2x2_I  \PTR_A_REG,\PTR_B_REG,0,1
.endm

.macro CALC_2x2_4  PTR_A_REG,PTR_B_REG 
   Calculate_2x2_I  \PTR_A_REG,\PTR_B_REG,0,0
   Calculate_2x2_I  \PTR_A_REG,\PTR_B_REG,1,0
   Calculate_2x2_I  \PTR_A_REG,\PTR_B_REG,2,0
   Calculate_2x2_I  \PTR_A_REG,\PTR_B_REG,3,1 
.endm

.macro CALC_2x1  PTR_A_REG,PTR_B_REG
   Calculate_2x1_I  \PTR_A_REG,\PTR_B_REG,0,1
.endm

.macro CALC_2x1_4  PTR_A_REG,PTR_B_REG 
   Calculate_2x1_I  \PTR_A_REG,\PTR_B_REG,0,0
   Calculate_2x1_I  \PTR_A_REG,\PTR_B_REG,1,0
   Calculate_2x1_I  \PTR_A_REG,\PTR_B_REG,2,0
   Calculate_2x1_I  \PTR_A_REG,\PTR_B_REG,3,1 
.endm

.macro CALC_1x4  PTR_A_REG,PTR_B_REG
   Calculate_4x1_I  \PTR_B_REG,\PTR_A_REG,0,1
.endm

.macro CALC_1x4_4  PTR_A_REG,PTR_B_REG 
   Calculate_4x1_I  \PTR_B_REG,\PTR_A_REG,0,0
   Calculate_4x1_I  \PTR_B_REG,\PTR_A_REG,1,0
   Calculate_4x1_I  \PTR_B_REG,\PTR_A_REG,2,0
   Calculate_4x1_I  \PTR_B_REG,\PTR_A_REG,3,1 
.endm

.macro CALC_1x2  PTR_A_REG,PTR_B_REG
   Calculate_2x1_I  \PTR_B_REG,\PTR_A_REG,0,1
.endm

.macro CALC_1x2_4  PTR_A_REG,PTR_B_REG 
   Calculate_2x1_I  \PTR_B_REG,\PTR_A_REG,0,0
   Calculate_2x1_I  \PTR_B_REG,\PTR_A_REG,1,0
   Calculate_2x1_I  \PTR_B_REG,\PTR_A_REG,2,0
   Calculate_2x1_I  \PTR_B_REG,\PTR_A_REG,3,1 
.endm

.macro CALC_1x1  PTR_A_REG,PTR_B_REG
   Calculate_1x1_I  \PTR_A_REG,\PTR_B_REG,0,1
.endm

.macro CALC_1x1_4  PTR_A_REG,PTR_B_REG 
   Calculate_1x1_I  \PTR_A_REG,\PTR_B_REG,0,0
   Calculate_1x1_I  \PTR_A_REG,\PTR_B_REG,1,0
   Calculate_1x1_I  \PTR_A_REG,\PTR_B_REG,2,0
   Calculate_1x1_I  \PTR_A_REG,\PTR_B_REG,3,1 
.endm
 

/**************************************STORAGE*************************************************/


.macro Multiply_8x1 vr1,vr2,vr3,vr4,va1,va2,va3,va4,vb1   
 #if defined(TRMMKERNEL)
        vfmdb    \vr1,\va1,\vb1    
        vfmdb    \vr2,\va2,\vb1    
        vfmdb    \vr3,\va3,\vb1  
        vfmdb    \vr4,\va4,\vb1   
 #else
        vfmadb    \vr1,\va1,\vb1,\vr1    
        vfmadb    \vr2,\va2,\vb1,\vr2   
        vfmadb    \vr3,\va3,\vb1,\vr3  
        vfmadb    \vr4,\va4,\vb1,\vr4   
 #endif         
.endm    

.macro Multiply_4x1 vr1,vr2, va1,va2, vb1   
 #if defined(TRMMKERNEL)
        vfmdb    \vr1,\va1,\vb1    
        vfmdb    \vr2,\va2,\vb1     
 #else
        vfmadb    \vr1,\va1,\vb1,\vr1    
        vfmadb    \vr2,\va2,\vb1,\vr2    
 #endif        
.endm

.macro Multiply_2x1 vr1, va1,vb1   
 #if defined(TRMMKERNEL)
        vfmdb    \vr1,\va1,\vb1     
 #else
        vfmadb    \vr1,\va1,\vb1,\vr1      
 #endif         
.endm   

 
.macro STORE_8x4  ALPHA_VECREG,CIJ_REG , LDC_BYTE_ORIGINAL , LV1 ,LV2
    la \LV1,0(\LDC_BYTE_ORIGINAL, \LDC_BYTE_ORIGINAL)
#if !defined(TRMMKERNEL)
        vl  %v5, 0(\CIJ_REG)
        vl %v1 ,  16(\CIJ_REG)
        vmrhf %v2,%v5,%v5 
        vmrhf %v4,%v1,%v1
        vmrlf  %v3,%v5,%v5
        vldeb  %v2, %v2
        vldeb  %v3, %v3
        vldeb  %v4, %v4 
        vmrlf  %v5,%v1,%v1 
        vldeb  %v5, %v5
#endif    
    Multiply_8x1 %v2,%v3,%v4,%v5,  %v16,%v17,%v18,%v19  ,\ALPHA_VECREG
    vledb  %v2, %v2,0,0  
    vledb  %v3, %v3,0,0 
    vledb  %v4, %v4,0,0
    vledb  %v5, %v5,0,0 
    vstef  %v2, 0(\CIJ_REG),0
    vstef  %v2, 4(\CIJ_REG),2
    vstef  %v3, 8(\CIJ_REG),0
    vstef  %v3, 12(\CIJ_REG),2
    vstef  %v4, 16(\CIJ_REG),0
    vstef  %v4, 20(\CIJ_REG),2
    vstef  %v5, 24(\CIJ_REG),0
    vstef  %v5, 28(\CIJ_REG),2
     

    la \LV2,0(\LV1,\LDC_BYTE_ORIGINAL )
#if !defined(TRMMKERNEL)    
    vl %v16,0(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vl %v17,16(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vmrhf %v2,%v16,%v16 
    vmrhf %v4,%v17,%v17
    vmrlf  %v3,%v16,%v16
    vldeb  %v2, %v2
    vldeb  %v3, %v3
    vldeb  %v4, %v4 
    vmrlf  %v5,%v17,%v17 
    vldeb  %v5, %v5
#endif
    Multiply_8x1 %v2,%v3,%v4,%v5,  %v20,%v21,%v22,%v23  ,\ALPHA_VECREG
    vledb  %v2, %v2,0,0  
    vledb  %v3, %v3,0,0 
    vledb  %v4, %v4,0,0
    vledb  %v5, %v5,0,0 
    vstef  %v2, 0(\CIJ_REG,\LDC_BYTE_ORIGINAL),0
    vstef  %v2, 4(\CIJ_REG,\LDC_BYTE_ORIGINAL),2
    vstef  %v3, 8(\CIJ_REG,\LDC_BYTE_ORIGINAL),0
    vstef  %v3, 12(\CIJ_REG,\LDC_BYTE_ORIGINAL),2
    vstef  %v4, 16(\CIJ_REG,\LDC_BYTE_ORIGINAL),0
    vstef  %v4, 20(\CIJ_REG,\LDC_BYTE_ORIGINAL),2
    vstef  %v5, 24(\CIJ_REG,\LDC_BYTE_ORIGINAL),0
    vstef  %v5, 28(\CIJ_REG,\LDC_BYTE_ORIGINAL),2

 #if !defined(TRMMKERNEL)
    vl %v17,0(\CIJ_REG,\LV1)
    vl %v18,16(\CIJ_REG,\LV1)
    vmrhf %v2,%v17,%v17 
    vmrhf %v4,%v18,%v18
    vmrlf  %v3,%v17,%v17
    vldeb  %v2, %v2
    vldeb  %v3, %v3
    vldeb  %v4, %v4 
    vmrlf  %v5,%v18,%v18 
    vldeb  %v5, %v5
#endif    
    Multiply_8x1 %v2,%v3,%v4,%v5,  %v24,%v25,%v26,%v27  ,\ALPHA_VECREG
    vledb  %v2, %v2,0,0  
    vledb  %v3, %v3,0,0 
    vledb  %v4, %v4,0,0
    vledb  %v5, %v5,0,0 
    vstef  %v2, 0(\CIJ_REG,\LV1),0
    vstef  %v2, 4(\CIJ_REG,\LV1),2
    vstef  %v3, 8(\CIJ_REG,\LV1),0
    vstef  %v3, 12(\CIJ_REG,\LV1),2
    vstef  %v4, 16(\CIJ_REG,\LV1),0
    vstef  %v4, 20(\CIJ_REG,\LV1),2
    vstef  %v5, 24(\CIJ_REG,\LV1),0
    vstef  %v5, 28(\CIJ_REG,\LV1),2

#if !defined(TRMMKERNEL)    
    vl %v16,0(\CIJ_REG,\LV2)
    vl %v17,16(\CIJ_REG,\LV2)
    vmrhf %v2,%v16,%v16 
    vmrhf %v4,%v17,%v17
    vmrlf  %v3,%v16,%v16
    vldeb  %v2, %v2
    vldeb  %v3, %v3
    vldeb  %v4, %v4 
    vmrlf  %v5,%v17,%v17 
    vldeb  %v5, %v5
#endif
    Multiply_8x1 %v2,%v3,%v4,%v5,  %v28,%v29,%v30,%v31  ,\ALPHA_VECREG
    vledb  %v2, %v2,0,0  
    vledb  %v3, %v3,0,0 
    vledb  %v4, %v4,0,0
    vledb  %v5, %v5,0,0 
    vstef  %v2, 0(\CIJ_REG,\LV2),0
    vstef  %v2, 4(\CIJ_REG,\LV2),2
    vstef  %v3, 8(\CIJ_REG,\LV2),0
    vstef  %v3, 12(\CIJ_REG,\LV2),2
    vstef  %v4, 16(\CIJ_REG,\LV2),0
    vstef  %v4, 20(\CIJ_REG,\LV2),2
    vstef  %v5, 24(\CIJ_REG,\LV2),0
    vstef  %v5, 28(\CIJ_REG,\LV2),2

    la \CIJ_REG,N8(\CIJ_REG)
 
.endm

.macro STORE_8x2  ALPHA_VECREG,CIJ_REG , LDC_BYTE_ORIGINAL
#if !defined(TRMMKERNEL)
        vl  %v5, 0(\CIJ_REG)
        vl %v1 ,  16(\CIJ_REG)
        vmrhf %v2,%v5,%v5 
        vmrhf %v4,%v1,%v1
        vmrlf  %v3,%v5,%v5
        vldeb  %v2, %v2
        vldeb  %v3, %v3
        vldeb  %v4, %v4 
        vmrlf  %v5,%v1,%v1 
        vldeb  %v5, %v5
#endif    
    Multiply_8x1 %v2,%v3,%v4,%v5,  %v16,%v17,%v18,%v19  ,\ALPHA_VECREG
    vledb  %v2, %v2,0,0  
    vledb  %v3, %v3,0,0 
    vledb  %v4, %v4,0,0
    vledb  %v5, %v5,0,0 
    vstef  %v2, 0(\CIJ_REG),0
    vstef  %v2, 4(\CIJ_REG),2
    vstef  %v3, 8(\CIJ_REG),0
    vstef  %v3, 12(\CIJ_REG),2
    vstef  %v4, 16(\CIJ_REG),0
    vstef  %v4, 20(\CIJ_REG),2
    vstef  %v5, 24(\CIJ_REG),0
    vstef  %v5, 28(\CIJ_REG),2
      
#if !defined(TRMMKERNEL)    
    vl %v16,0(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vl %v17,16(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vmrhf %v2,%v16,%v16 
    vmrhf %v4,%v17,%v17
    vmrlf  %v3,%v16,%v16
    vldeb  %v2, %v2
    vldeb  %v3, %v3
    vldeb  %v4, %v4 
    vmrlf  %v5,%v17,%v17 
    vldeb  %v5, %v5
#endif
    Multiply_8x1 %v2,%v3,%v4,%v5,  %v20,%v21,%v22,%v23  ,\ALPHA_VECREG
    vledb  %v2, %v2,0,0  
    vledb  %v3, %v3,0,0 
    vledb  %v4, %v4,0,0
    vledb  %v5, %v5,0,0 
    vstef  %v2, 0(\CIJ_REG,\LDC_BYTE_ORIGINAL),0
    vstef  %v2, 4(\CIJ_REG,\LDC_BYTE_ORIGINAL),2
    vstef  %v3, 8(\CIJ_REG,\LDC_BYTE_ORIGINAL),0
    vstef  %v3, 12(\CIJ_REG,\LDC_BYTE_ORIGINAL),2
    vstef  %v4, 16(\CIJ_REG,\LDC_BYTE_ORIGINAL),0
    vstef  %v4, 20(\CIJ_REG,\LDC_BYTE_ORIGINAL),2
    vstef  %v5, 24(\CIJ_REG,\LDC_BYTE_ORIGINAL),0
    vstef  %v5, 28(\CIJ_REG,\LDC_BYTE_ORIGINAL),2
 
   la \CIJ_REG,N8(\CIJ_REG)
 
.endm

.macro STORE_8x1  ALPHA_VECREG,CIJ_REG , LDC_BYTE_ORIGINAL
    
#if !defined(TRMMKERNEL)
        vl  %v5, 0(\CIJ_REG)
        vl %v1 ,  16(\CIJ_REG)
        vmrhf %v2,%v5,%v5 
        vmrhf %v4,%v1,%v1
        vmrlf  %v3,%v5,%v5
        vldeb  %v2, %v2
        vldeb  %v3, %v3
        vldeb  %v4, %v4 
        vmrlf  %v5,%v1,%v1 
        vldeb  %v5, %v5
#endif    
    Multiply_8x1 %v2,%v3,%v4,%v5,  %v16,%v17,%v18,%v19  ,\ALPHA_VECREG
    vledb  %v2, %v2,0,0  
    vledb  %v3, %v3,0,0 
    vledb  %v4, %v4,0,0
    vledb  %v5, %v5,0,0 
    vstef  %v2, 0(\CIJ_REG),0
    vstef  %v2, 4(\CIJ_REG),2
    vstef  %v3, 8(\CIJ_REG),0
    vstef  %v3, 12(\CIJ_REG),2
    vstef  %v4, 16(\CIJ_REG),0
    vstef  %v4, 20(\CIJ_REG),2
    vstef  %v5, 24(\CIJ_REG),0
    vstef  %v5, 28(\CIJ_REG),2

    la \CIJ_REG,N8(\CIJ_REG)
.endm


.macro STORE_4x4  ALPHA_VECREG,CIJ_REG , LDC_BYTE_ORIGINAL,   LV1 ,LV2
    la \LV1,0(\LDC_BYTE_ORIGINAL, \LDC_BYTE_ORIGINAL)
#if !defined(TRMMKERNEL)
        vl  %v5, 0(\CIJ_REG) 
        vmrhf %v1,%v5,%v5  
        vmrlf  %v2,%v5,%v5
        vldeb  %v1, %v1
        vldeb  %v2, %v2
        
#endif    
    Multiply_4x1 %v1,%v2 ,  %v16,%v17   ,\ALPHA_VECREG
    vledb  %v1, %v1,0,0  
    vledb  %v2, %v2,0,0  
    vstef  %v1, 0(\CIJ_REG),0
    vstef  %v1, 4(\CIJ_REG),2
    vstef  %v2, 8(\CIJ_REG),0
    vstef  %v2, 12(\CIJ_REG),2
    
    la \LV2,0(\LV1,\LDC_BYTE_ORIGINAL )
#if !defined(TRMMKERNEL)    
    vl  %v5, 0(\CIJ_REG,\LDC_BYTE_ORIGINAL )
    vmrhf %v16,%v5,%v5  
    vmrlf  %v17,%v5,%v5
    vldeb  %v16, %v16
    vldeb  %v17, %v17
#endif
    Multiply_4x1 %v16,%v17 ,  %v20,%v21  ,\ALPHA_VECREG
    vledb  %v1, %v16,0,0  
    vledb  %v2, %v17,0,0  
    vstef  %v1, 0(\CIJ_REG,\LDC_BYTE_ORIGINAL ),0
    vstef  %v1, 4(\CIJ_REG,\LDC_BYTE_ORIGINAL ),2
    vstef  %v2, 8(\CIJ_REG,\LDC_BYTE_ORIGINAL ),0
    vstef  %v2, 12(\CIJ_REG,\LDC_BYTE_ORIGINAL ),2 

 #if !defined(TRMMKERNEL)
    vl  %v5, 0(\CIJ_REG,\LV1 )
    vmrhf %v16,%v5,%v5  
    vmrlf  %v17,%v5,%v5
    vldeb  %v16, %v16
    vldeb  %v17, %v17
#endif    
    Multiply_4x1 %v16,%v17 ,  %v24,%v25 ,\ALPHA_VECREG
    vledb  %v1, %v16,0,0  
    vledb  %v2, %v17,0,0  
    vstef  %v1, 0(\CIJ_REG,\LV1 ),0
    vstef  %v1, 4(\CIJ_REG,\LV1 ),2
    vstef  %v2, 8(\CIJ_REG,\LV1 ),0
    vstef  %v2, 12(\CIJ_REG,\LV1 ),2    

#if !defined(TRMMKERNEL)    
    vl  %v5, 0(\CIJ_REG,\LV2 )
    vmrhf %v16,%v5,%v5  
    vmrlf  %v17,%v5,%v5
    vldeb  %v16, %v16
    vldeb  %v17, %v17 
#endif
    Multiply_4x1 %v16,%v17,   %v28,%v29   ,\ALPHA_VECREG
    vledb  %v1, %v16,0,0  
    vledb  %v2, %v17,0,0  
    vstef  %v1, 0(\CIJ_REG,\LV2 ),0
    vstef  %v1, 4(\CIJ_REG,\LV2 ),2
    vstef  %v2, 8(\CIJ_REG,\LV2 ),0
    vstef  %v2, 12(\CIJ_REG,\LV2 ),2   

   la \CIJ_REG,N4(\CIJ_REG)
 
.endm


.macro STORE_4x2  ALPHA_VECREG,CIJ_REG , LDC_BYTE_ORIGINAL
 
#if !defined(TRMMKERNEL)
        vl  %v5, 0(\CIJ_REG) 
        vmrhf %v1,%v5,%v5  
        vmrlf  %v2,%v5,%v5
        vldeb  %v1, %v1
        vldeb  %v2, %v2
        
#endif    
    Multiply_4x1 %v1,%v2 ,  %v16,%v17   ,\ALPHA_VECREG
    vledb  %v1, %v1,0,0  
    vledb  %v2, %v2,0,0  
    vstef  %v1, 0(\CIJ_REG),0
    vstef  %v1, 4(\CIJ_REG),2
    vstef  %v2, 8(\CIJ_REG),0
    vstef  %v2, 12(\CIJ_REG),2
     
#if !defined(TRMMKERNEL)    
        vl  %v5, 0(\CIJ_REG,\LDC_BYTE_ORIGINAL )
        vmrhf %v16,%v5,%v5  
        vmrlf  %v17,%v5,%v5
        vldeb  %v16, %v16
        vldeb  %v17, %v17
#endif
    Multiply_4x1 %v16,%v17 ,  %v20,%v21  ,\ALPHA_VECREG
    vledb  %v1, %v16,0,0  
    vledb  %v2, %v17,0,0  
    vstef  %v1, 0(\CIJ_REG,\LDC_BYTE_ORIGINAL ),0
    vstef  %v1, 4(\CIJ_REG,\LDC_BYTE_ORIGINAL ),2
    vstef  %v2, 8(\CIJ_REG,\LDC_BYTE_ORIGINAL ),0
    vstef  %v2, 12(\CIJ_REG,\LDC_BYTE_ORIGINAL ),2 
 

    la \CIJ_REG,N4(\CIJ_REG)
 
.endm

.macro STORE_4x1  ALPHA_FLOAT,CIJ_REG , LDC_BYTE_ORIGINAL 
    ledbr %f7,\ALPHA_FLOAT  
#if  defined(TRMMKERNEL) 
     meebr %f1,%f7
     meebr %f2,%f7
     meebr %f3,%f7
     meebr %f4,%f7
     ste %f1,0(\CIJ_REG) 
     ste %f2,4(\CIJ_REG )  
     ste %f3,8(\CIJ_REG ) 
     ste %f4,12(\CIJ_REG)  
#else    
     le %f5,0(\CIJ_REG)
     maebr %f5,%f1,%f7
     ste %f5,0(\CIJ_REG) 
     

     le %f6,4(\CIJ_REG ) 
     maebr %f6,%f2,%f7
     ste %f6,4(\CIJ_REG ) 
     
     le %f5,8(\CIJ_REG)
     maebr %f5,%f3,%f7
     ste %f5,8(\CIJ_REG)
     
     le %f6,12(\CIJ_REG)
     maebr %f6,%f4,%f7
     ste %f6,12(\CIJ_REG) 
#endif    
 
      la \CIJ_REG,N4(\CIJ_REG)
 
.endm

.macro STORE_2x2  ALPHA_VECREG,CIJ_REG , LDC_BYTE_ORIGINAL
 
#if !defined(TRMMKERNEL)
     vlef	%v1,0(\CIJ_REG) ,0
     vlef	%v1,4(\CIJ_REG) ,2
     vldeb	%v1,%v1 
        
#endif    
    Multiply_2x1 %v1, %v16,\ALPHA_VECREG
    vledb  %v1, %v1,0,0   
    vstef  %v1, 0(\CIJ_REG),0
    vstef  %v1, 4(\CIJ_REG),2 
     
#if !defined(TRMMKERNEL)    
    vlef	%v16,0(\CIJ_REG,\LDC_BYTE_ORIGINAL ) ,0
    vlef	%v16,4(\CIJ_REG,\LDC_BYTE_ORIGINAL ),2
    vldeb	%v16,%v16 
#endif
    Multiply_2x1 %v16, %v20,\ALPHA_VECREG
    vledb  %v1, %v16,0,0   
    vstef  %v1, 0(\CIJ_REG,\LDC_BYTE_ORIGINAL ),0
    vstef  %v1, 4(\CIJ_REG,\LDC_BYTE_ORIGINAL ),2 

    la \CIJ_REG,N2(\CIJ_REG)
 
.endm


.macro STORE_2x1  ALPHA_FLOAT,CIJ_REG , LDC_BYTE_ORIGINAL
     ledbr %f3,\ALPHA_FLOAT
#if  defined(TRMMKERNEL) 
     meebr %f1,%f3
     meebr %f2,%f3
     ste %f1,0(\CIJ_REG) 
     ste %f2,4(\CIJ_REG)  
#else    
     le %f4,0(\CIJ_REG)
     le %f5,4(\CIJ_REG)
     maebr %f4,%f1,%f3
     maebr %f5,%f2,%f3
     ste %f4,0(\CIJ_REG) 
     ste %f5,4(\CIJ_REG) 
#endif      

    la \CIJ_REG,N2(\CIJ_REG)
.endm


/*STORE C1X1*/
.macro STORE_1x1  ALPHA_FLOAT,CIJ_REG,LDC_BYTE_ORIGINAL  
     ledbr %f3,\ALPHA_FLOAT
#if  defined(TRMMKERNEL) 
     meebr %f1,%f3
     ste %f1,0(\CIJ_REG)  
#else    
     le %f2,0(\CIJ_REG)
     maebr %f2,%f1,%f3
     ste %f2,0(\CIJ_REG) 
#endif     
     la \CIJ_REG,N1(\CIJ_REG) 
.endm

/*reversed ones*/

.macro STORE_2x4  ALPHA_VECREG,CIJ_REG , LDC_BYTE_ORIGINAL , LV1 ,LV2
#if !defined(TRMMKERNEL)
     vlef	%v1,0(\CIJ_REG) ,0
     vlef	%v1,0(\CIJ_REG,\LDC_BYTE_ORIGINAL) ,2
     vldeb	%v1,%v1  
#endif    
    la \LV1,0(\LDC_BYTE_ORIGINAL, \LDC_BYTE_ORIGINAL) 
    Multiply_2x1 %v1,  %v16 ,\ALPHA_VECREG

    la \LV2,0(\LV1,\LDC_BYTE_ORIGINAL )
    vledb  %v1, %v1,0,0   
    vstef  %v1, 0(\CIJ_REG),0
    vstef  %v1, 0(\CIJ_REG,\LDC_BYTE_ORIGINAL),2  
     
#if !defined(TRMMKERNEL)    
    vlef	%v16,0(\CIJ_REG,\LV1  ) ,0
    vlef	%v16,0(\CIJ_REG,\LV2 ),2
    vldeb	%v16,%v16 
#endif
    Multiply_2x1 %v16,   %v17,\ALPHA_VECREG
    vledb  %v1, %v16,0,0   
    vstef  %v1, 0(\CIJ_REG ,\LV1 ),0
    vstef  %v1, 0(\CIJ_REG,\LV2  ),2 
/*2nd*/
#if !defined(TRMMKERNEL)
     vlef	%v1,4(\CIJ_REG) ,0
     vlef	%v1,4(\CIJ_REG,\LDC_BYTE_ORIGINAL) ,2
     vldeb	%v1,%v1 
        
#endif     
    Multiply_2x1 %v1,  %v20 ,\ALPHA_VECREG 
    vledb  %v1, %v1,0,0   
    vstef  %v1, 4(\CIJ_REG),0
    vstef  %v1,4(\CIJ_REG,\LDC_BYTE_ORIGINAL),2  
     
#if !defined(TRMMKERNEL)    
    vlef	%v16,4(\CIJ_REG,\LV1  ) ,0
    vlef	%v16,4(\CIJ_REG,\LV2 ),2
    vldeb	%v16,%v16 
#endif
    Multiply_2x1 %v16,   %v21,\ALPHA_VECREG
    vledb  %v1, %v16,0,0   
    vstef  %v1, 4(\CIJ_REG ,\LV1 ),0
    vstef  %v1, 4(\CIJ_REG,\LV2  ),2 
    
    la \CIJ_REG,N2(\CIJ_REG)

.endm

.macro STORE_1x4  ALPHA_FLOAT,CIJ_REG , LDC_BYTE_ORIGINAL  , LV1 ,LV2
    
    la \LV1,0(\LDC_BYTE_ORIGINAL, \LDC_BYTE_ORIGINAL)
    ledbr %f7,\ALPHA_FLOAT 
    la \LV2,0(\LV1,\LDC_BYTE_ORIGINAL )
#if  defined(TRMMKERNEL) 
     meebr %f1,%f7
     meebr %f2,%f7
     meebr %f3,%f7
     meebr %f4,%f7
     ste %f1,0(\CIJ_REG) 
     ste %f2,0(\CIJ_REG, \LDC_BYTE_ORIGINAL)  
     ste %f3,0(\CIJ_REG, \LV1) 
     ste %f4,0(\CIJ_REG, \LV2)  
#else    
     le %f5,0(\CIJ_REG)
     maebr %f5,%f1,%f7
     ste %f5,0(\CIJ_REG) 
   
     le %f6,0(\CIJ_REG,\LDC_BYTE_ORIGINAL) 
     maebr %f6,%f2,%f7
     ste %f6,0(\CIJ_REG,\LDC_BYTE_ORIGINAL) 
     
     le %f5,0(\CIJ_REG, \LV1)
     maebr %f5,%f3,%f7
     ste %f5,0(\CIJ_REG, \LV1)
     
     le %f6,0(\CIJ_REG, \LV2)
     maebr %f6,%f4,%f7
     ste %f6,0(\CIJ_REG, \LV2) 
#endif  
 
    la \CIJ_REG,N1(\CIJ_REG)

.endm

 .macro STORE_1x2  ALPHA_FLOAT,CIJ_REG , LDC_BYTE_ORIGINAL
     ledbr %f3,\ALPHA_FLOAT
#if  defined(TRMMKERNEL) 
     meebr %f1,%f3
     meebr %f2,%f3
     ste %f1,0(\CIJ_REG) 
     ste %f2,0(\CIJ_REG,\LDC_BYTE_ORIGINAL) 
#else    
     le %f4,0(\CIJ_REG)
     maebr %f4,%f1,%f3
     ste %f4,0(\CIJ_REG)  

     le %f5,0(\CIJ_REG,\LDC_BYTE_ORIGINAL) 
     maebr %f5,%f2,%f3
     ste %f5,0(\CIJ_REG,\LDC_BYTE_ORIGINAL) 
#endif         
 
      
    la \CIJ_REG,N1(\CIJ_REG)

.endm




/****************************TRMM POINTER REFRESH MACROSES*************************/

.macro RefreshPointers  PTR_A,PTR_B,OFF_VAL,B_VAL,C_A,C_B
    #if (defined(LEFT) &&  defined(TRANSA)) ||  (!defined(LEFT) && !defined(TRANSA))
               /*  ptrbb = bb;*/
            lgr \PTR_B,\B_VAL      /*refresh BPOINT*/

    #else
             /*  ptrba  =ptrba+ off*C_A;
              ptrbb = bb + off*C_B;*/
.if \C_B==4 
                .if \C_A==8 
                        sllg \PTR_B, \OFF_VAL,4
                        la \PTR_A,0(\PTR_A,\PTR_B) /*ptrba+off*4*/
                        agr \PTR_A,\PTR_B /*ptrba+off*4**/
                        la \PTR_B,0(\B_VAL,\PTR_B)        
                .elseif \C_A==4
                        sllg \PTR_B, \OFF_VAL,4 
                        agr \PTR_A,\PTR_B /*ptrba+off*4**/
                        la \PTR_B,0(\B_VAL,\PTR_B)        /*refresh BPOINT*/
                .elseif \C_A==2
                        sllg \PTR_B, \OFF_VAL,3 
                        la \PTR_A,0(\PTR_A,\PTR_B) /*ptrba+off*2**/
                        agr \PTR_B, \PTR_B   
                        la \PTR_B,0(\B_VAL,\PTR_B)        /*refresh BPOINT*/

                .elseif \C_A==1
                        sllg \PTR_B, \OFF_VAL,2 
                        agr \PTR_A,\PTR_B /*ptrba+off*4**/
                        sllg \PTR_B, \OFF_VAL,4   
                        la \PTR_B,0(\B_VAL,\PTR_B)        /*refresh BPOINT*/
                .endif

.elseif \C_B==2
                .if \C_A==8 
                        sllg \PTR_B, \OFF_VAL,5 
                        agr \PTR_A,\PTR_B /*ptrba+off*8**/
                        sllg \PTR_B, \OFF_VAL,3   
                        la \PTR_B,0(\B_VAL,\PTR_B)        /*refresh BPOINT*/
                .elseif \C_A==4
                        sllg \PTR_B, \OFF_VAL,3 
                        la \PTR_A,0(\PTR_A,\PTR_B) /*ptrba+off*2**/
                        agr \PTR_A,\PTR_B /*ptrba+off*2**/  
                        la \PTR_B,0(\B_VAL,\PTR_B)        /*refresh BPOINT*/
                .elseif \C_A==2
                        sllg \PTR_B, \OFF_VAL,3 
                        agr \PTR_A,\PTR_B /*ptrba+off*2**/ 
                        la \PTR_B,0(\B_VAL,\PTR_B)        /*refresh BPOINT*/ 
                .elseif \C_A==1
                        sllg \PTR_B, \OFF_VAL,2 
                        la \PTR_A,0(\PTR_A,\PTR_B) /*ptrba+off*1**/
                        agr \PTR_B,\PTR_B /* off+off**/  
                        la \PTR_B,0(\B_VAL,\PTR_B)        /*refresh BPOINT*/
                .endif

.elseif \C_B==1
                .if \C_A==8 
                        sllg \PTR_B, \OFF_VAL,5 
                        agr \PTR_A,\PTR_B /*ptrba+off*8**/
                        sllg \PTR_B, \OFF_VAL,2 
                        la \PTR_B,0(\B_VAL,\PTR_B)        /*refresh BPOINT*/
                .elseif \C_A==4
                        sllg \PTR_B, \OFF_VAL,4 
                        agr \PTR_A,\PTR_B /*ptrba+off*4**/
                        sllg \PTR_B, \OFF_VAL,2 
                        la \PTR_B,0(\B_VAL,\PTR_B)        /*refresh BPOINT*/
                .elseif \C_A==2
                        sllg \PTR_B, \OFF_VAL,2 
                        la \PTR_A,0(\PTR_A,\PTR_B) /*ptrba+off*1**/
                        agr \PTR_A,\PTR_B /*ptrba+off*1**/
                        la \PTR_B,0(\B_VAL,\PTR_B)        /*refresh BPOINT*/

                .elseif \C_A==1
                        sllg \PTR_B, \OFF_VAL,2 
                        agr \PTR_A,\PTR_B /*ptrba+off*1**/ 
                        la \PTR_B,0(\B_VAL,\PTR_B)        /*refresh BPOINT*/
                .endif
.endif
             
 
    #endif 
.endm

/**/
.macro RefreshTempBk TEMP_VAL,BK_VAL,OFF_VAL,INCR_A,INCR_B
    #if (defined(LEFT) && !defined(TRANSA)) ||  (!defined(LEFT) && defined(TRANSA))
                            /* temp = bk-off;*/
           sgrk \TEMP_VAL,\BK_VAL,\OFF_VAL

    #elif defined(LEFT)
                            /* temp = off+INCR_A;       // number of values in A */
           la \TEMP_VAL,\INCR_A(\OFF_VAL)
    #else
                            /* temp = off+INCR_B        // number of values in B*/
           la \TEMP_VAL,\INCR_B(\OFF_VAL)
    #endif

.endm
 

.macro RefreshPointersAndOFF TEMP_VAL,BK_VAL,OFF_VAL,PTR_B,PTR_A,C_A,C_B

    #if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
                    /*temp = bk - off;*/
                    sgrk \TEMP_VAL,\BK_VAL,\OFF_VAL
    #ifdef LEFT
                    /*temp -= 8; // number of values in A*/
                    lay \TEMP_VAL,-\C_A(\TEMP_VAL)
    #else
                    /*temp -= 4; // number of values in B*/
                   lay \TEMP_VAL,-\C_B(\TEMP_VAL)
    #endif
                    /*ptrba += temp*C_A;
                    ptrbb += temp*C_B;*/ 
                .if  \C_A==8 
                        sllg \TEMP_VAL, \TEMP_VAL,5     
                .elseif \C_A==4
                        sllg \TEMP_VAL, \TEMP_VAL,4 /*temp*4*/ 
                .elseif \C_A==2
                        sllg \TEMP_VAL, \TEMP_VAL,3 /*temp*2*/  
                .elseif \C_A==1
                        sllg \TEMP_VAL, \TEMP_VAL,2 /*temp*1*/  
                .endif
                la \PTR_A,0(\PTR_A,\TEMP_VAL) /*ptrba+temp*C_A*/ 
                /*we do not need to refresh ptrbb. so lets ignore it*/

    #endif

    #ifdef LEFT
                    /*off += 8; // number of values in A*/
                    aghi \OFF_VAL,\C_A
    #endif
.endm