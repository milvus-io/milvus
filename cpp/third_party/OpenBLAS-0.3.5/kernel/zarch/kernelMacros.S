/*********************************KERNEL 8x4***********************************************/
/*Zero C block Vectors*/
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

/*Calculate for 8x4 C blocks*/
.macro CALC_8x4  PTR_A_REG,PTR_B_REG
        vlrepg %v7, 0(\PTR_B_REG)  
        vlrepg %v1,8(\PTR_B_REG)    
        vl  %v2, 0(\PTR_A_REG)     
        vl  %v3, 16(\PTR_A_REG)     
        vl  %v4, 32(\PTR_A_REG)   
        vl  %v5, 48(\PTR_A_REG)      
        vfmadb    %v16,%v2,%v7,%v16   
        vfmadb    %v17,%v3,%v7,%v17   
        vfmadb    %v18,%v4,%v7,%v18  
        vfmadb    %v19,%v5,%v7,%v19     
        vfmadb    %v20,%v2,%v1,%v20   
        vfmadb    %v21,%v3,%v1,%v21 
        vlrepg %v7,16(\PTR_B_REG)    
        vfmadb    %v22,%v4,%v1,%v22  
        vfmadb    %v23,%v5,%v1,%v23   
        vlrepg %v1,24(\PTR_B_REG)     
        vfmadb    %v24,%v2,%v7,%v24   
        vfmadb    %v25,%v3,%v7,%v25   
        vfmadb    %v26,%v4,%v7,%v26  
        la \PTR_A_REG, 64(\PTR_A_REG)    
        vfmadb    %v27,%v5,%v7,%v27        
        vfmadb    %v28,%v2,%v1,%v28   
        vfmadb    %v29,%v3,%v1,%v29   
        la \PTR_B_REG, 32(\PTR_B_REG)    
        vfmadb    %v30,%v4,%v1,%v30  
        vfmadb    %v31,%v5,%v1,%v31  
.endm
 
/*Calculate for 8x4_4 C blocks*/
.macro CALC_8x4_4  PTR_A_REG,PTR_B_REG 
        vlrepg %v7, 0(\PTR_B_REG)  
        vlrepg %v1,8(\PTR_B_REG)    
        vl  %v2, 0(\PTR_A_REG)     
        vl  %v3, 16(\PTR_A_REG)     
        vl  %v4, 32(\PTR_A_REG)   
        vl  %v5, 48(\PTR_A_REG)      
        vfmadb    %v16,%v2,%v7,%v16   
        vfmadb    %v17,%v3,%v7,%v17   
        vfmadb    %v18,%v4,%v7,%v18  
        vfmadb    %v19,%v5,%v7,%v19     
        vfmadb    %v20,%v2,%v1,%v20   
        vfmadb    %v21,%v3,%v1,%v21 
        vlrepg %v7,16(\PTR_B_REG)    
        vfmadb    %v22,%v4,%v1,%v22  
        vfmadb    %v23,%v5,%v1,%v23   
        vlrepg %v1,24(\PTR_B_REG)     
        vfmadb    %v24,%v2,%v7,%v24   
        vfmadb    %v25,%v3,%v7,%v25   
        vfmadb    %v26,%v4,%v7,%v26      
        vfmadb    %v27,%v5,%v7,%v27        
        vfmadb    %v28,%v2,%v1,%v28   
        vfmadb    %v29,%v3,%v1,%v29      
        vfmadb    %v30,%v4,%v1,%v30  
        vfmadb    %v31,%v5,%v1,%v31  

        vlrepg %v7, 32(\PTR_B_REG)  
        vlrepg %v1,40(\PTR_B_REG)    
        vl  %v2, 64(\PTR_A_REG)     
        vl  %v3, 80(\PTR_A_REG)     
        vl  %v4, 96(\PTR_A_REG)   
        vl  %v5, 112(\PTR_A_REG)      
        vfmadb    %v16,%v2,%v7,%v16   
        vfmadb    %v17,%v3,%v7,%v17   
        vfmadb    %v18,%v4,%v7,%v18  
        vfmadb    %v19,%v5,%v7,%v19     
        vfmadb    %v20,%v2,%v1,%v20   
        vfmadb    %v21,%v3,%v1,%v21 
        vlrepg %v7,48(\PTR_B_REG)    
        vfmadb    %v22,%v4,%v1,%v22  
        vfmadb    %v23,%v5,%v1,%v23   
        vlrepg %v1,56(\PTR_B_REG)     
        vfmadb    %v24,%v2,%v7,%v24   
        vfmadb    %v25,%v3,%v7,%v25   
        vfmadb    %v26,%v4,%v7,%v26      
        vfmadb    %v27,%v5,%v7,%v27        
        vfmadb    %v28,%v2,%v1,%v28   
        vfmadb    %v29,%v3,%v1,%v29      
        vfmadb    %v30,%v4,%v1,%v30  
        vfmadb    %v31,%v5,%v1,%v31  

        vlrepg %v7, 64(\PTR_B_REG)  
        vlrepg %v1,72(\PTR_B_REG)    
        vl  %v2, 128(\PTR_A_REG)     
        vl  %v3, 144(\PTR_A_REG)     
        vl  %v4, 160(\PTR_A_REG)   
        vl  %v5, 176(\PTR_A_REG)      
        vfmadb    %v16,%v2,%v7,%v16   
        vfmadb    %v17,%v3,%v7,%v17   
        vfmadb    %v18,%v4,%v7,%v18  
        vfmadb    %v19,%v5,%v7,%v19     
        vfmadb    %v20,%v2,%v1,%v20   
        vfmadb    %v21,%v3,%v1,%v21 
        vlrepg %v7,80(\PTR_B_REG)    
        vfmadb    %v22,%v4,%v1,%v22  
        vfmadb    %v23,%v5,%v1,%v23   
        vlrepg %v1,88(\PTR_B_REG)     
        vfmadb    %v24,%v2,%v7,%v24   
        vfmadb    %v25,%v3,%v7,%v25   
        vfmadb    %v26,%v4,%v7,%v26      
        vfmadb    %v27,%v5,%v7,%v27        
        vfmadb    %v28,%v2,%v1,%v28   
        vfmadb    %v29,%v3,%v1,%v29      
        vfmadb    %v30,%v4,%v1,%v30  
        vfmadb    %v31,%v5,%v1,%v31  

        vlrepg %v7, 96(\PTR_B_REG)  
        vlrepg %v1,104(\PTR_B_REG)    
        vl  %v2, 192(\PTR_A_REG)     
        vl  %v3, 208(\PTR_A_REG)     
        vl  %v4, 224(\PTR_A_REG)   
        vl  %v5, 240(\PTR_A_REG)      
        vfmadb    %v16,%v2,%v7,%v16   
        vfmadb    %v17,%v3,%v7,%v17   
        vfmadb    %v18,%v4,%v7,%v18  
        vfmadb    %v19,%v5,%v7,%v19     
        vfmadb    %v20,%v2,%v1,%v20   
        vfmadb    %v21,%v3,%v1,%v21 
        vlrepg %v7,112(\PTR_B_REG)    
        vfmadb    %v22,%v4,%v1,%v22  
        vfmadb    %v23,%v5,%v1,%v23   
        vlrepg %v1,120(\PTR_B_REG)     
        vfmadb    %v24,%v2,%v7,%v24   
        vfmadb    %v25,%v3,%v7,%v25   
        vfmadb    %v26,%v4,%v7,%v26      
        vfmadb    %v27,%v5,%v7,%v27   
        la \PTR_B_REG, 128(\PTR_B_REG) 
        vfmadb    %v28,%v2,%v1,%v28   
        vfmadb    %v29,%v3,%v1,%v29      
        vfmadb    %v30,%v4,%v1,%v30   
        la \PTR_A_REG, 256(\PTR_A_REG)  
        vfmadb    %v31,%v5,%v1,%v31  

.endm
 

/*STORE C8X4*/
.macro STORE_8x4  ALPHA_VECREG,CIJ_REG , LDC_BYTE_ORIGINAL
   
    /*add LDC_BYTE_reg=LDC_BYTE_original<<1 */  
    la LOCAL_VAR1,0(\LDC_BYTE_ORIGINAL, \LDC_BYTE_ORIGINAL)
    vl %v1,0(\CIJ_REG)
    vfmadb    %v1,%v16,\ALPHA_VECREG,%v1 
    vst %v1,0(\CIJ_REG)
      
    vl %v2,16(\CIJ_REG)
    vfmadb    %v2,%v17,\ALPHA_VECREG,%v2 
    vst %v2,16(\CIJ_REG)

    vl %v3,32(\CIJ_REG)
    vfmadb    %v3,%v18,\ALPHA_VECREG,%v3 
    vst %v3,32(\CIJ_REG)

    vl %v4,48(\CIJ_REG)
    vfmadb    %v4,%v19,\ALPHA_VECREG,%v4 
    vst %v4,48(\CIJ_REG)
    
    la LOCAL_VAR2,0(LOCAL_VAR1,\LDC_BYTE_ORIGINAL )
    

    /*add c LDC_BYTE*/
    vl %v1,0(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vfmadb    %v1,%v20,\ALPHA_VECREG,%v1 
    vst %v1,0(\CIJ_REG,\LDC_BYTE_ORIGINAL)

    vl %v2,16(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vfmadb    %v2,%v21,\ALPHA_VECREG,%v2 
    vst %v2,16(\CIJ_REG,\LDC_BYTE_ORIGINAL)
 

    vl %v3,32(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vfmadb    %v3,%v22,\ALPHA_VECREG,%v3 
    vst %v3,32(\CIJ_REG,\LDC_BYTE_ORIGINAL)

    vl %v4,48(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vfmadb    %v4,%v23,\ALPHA_VECREG,%v4 
    vst %v4,48(\CIJ_REG,\LDC_BYTE_ORIGINAL)


    vl %v1,0(\CIJ_REG,LOCAL_VAR1)
    vfmadb    %v1,%v24,\ALPHA_VECREG,%v1 
    vst %v1,0(\CIJ_REG,LOCAL_VAR1)

    vl %v2,16(\CIJ_REG,LOCAL_VAR1)
    vfmadb    %v2,%v25,\ALPHA_VECREG,%v2 
    vst %v2,16(\CIJ_REG,LOCAL_VAR1)

    vl %v3,32(\CIJ_REG,LOCAL_VAR1)
    vfmadb    %v3,%v26,\ALPHA_VECREG,%v3
    vst %v3,32(\CIJ_REG,LOCAL_VAR1)

    vl %v4,48(\CIJ_REG,LOCAL_VAR1)
    vfmadb    %v4,%v27,\ALPHA_VECREG,%v4 
    vst %v4,48(\CIJ_REG,LOCAL_VAR1)

    
    vl %v1,0(\CIJ_REG,LOCAL_VAR2)
    vfmadb    %v1,%v28,\ALPHA_VECREG,%v1 
    vst %v1,0(\CIJ_REG,LOCAL_VAR2)

    vl %v2,16(\CIJ_REG,LOCAL_VAR2)
    vfmadb    %v2,%v29,\ALPHA_VECREG,%v2 
    vst %v2,16(\CIJ_REG,LOCAL_VAR2)

    vl %v3,32(\CIJ_REG,LOCAL_VAR2)
    vfmadb    %v3,%v30,\ALPHA_VECREG,%v3 
    vst %v3,32(\CIJ_REG,LOCAL_VAR2)

    vl %v4,48(\CIJ_REG,LOCAL_VAR2)
    vfmadb    %v4,%v31,\ALPHA_VECREG,%v4
    vst %v4,48(\CIJ_REG,LOCAL_VAR2)

   la \CIJ_REG,64(\CIJ_REG)
 
.endm

/*STORE TRMM C8X4*/
.macro STORE_TRMM_8x4  ALPHA_VECREG,CIJ_REG , LDC_BYTE_ORIGINAL
   
    /*add LDC_BYTE_reg=LDC_BYTE_original<<1 */  
    la LOCAL_VAR1,0(\LDC_BYTE_ORIGINAL, \LDC_BYTE_ORIGINAL)
    vfmdb    %v1,%v16,\ALPHA_VECREG
    vst %v1,0(\CIJ_REG)
      
    vfmdb    %v2,%v17,\ALPHA_VECREG
    vst %v2,16(\CIJ_REG)
    vfmdb    %v3,%v18,\ALPHA_VECREG
    vst %v3,32(\CIJ_REG)
    vfmdb    %v4,%v19,\ALPHA_VECREG
    vst %v4,48(\CIJ_REG)
    
    la LOCAL_VAR2,0(LOCAL_VAR1,\LDC_BYTE_ORIGINAL )
    
    /*add c LDC_BYTE*/
    vfmdb    %v1,%v20,\ALPHA_VECREG
    vst %v1,0(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vfmdb    %v2,%v21,\ALPHA_VECREG
    vst %v2,16(\CIJ_REG,\LDC_BYTE_ORIGINAL)
 
    vfmdb    %v3,%v22,\ALPHA_VECREG
    vst %v3,32(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vfmdb    %v4,%v23,\ALPHA_VECREG
    vst %v4,48(\CIJ_REG,\LDC_BYTE_ORIGINAL)

    vfmdb    %v1,%v24,\ALPHA_VECREG
    vst %v1,0(\CIJ_REG,LOCAL_VAR1)
    vfmdb    %v2,%v25,\ALPHA_VECREG
    vst %v2,16(\CIJ_REG,LOCAL_VAR1)
    vfmdb    %v3,%v26,\ALPHA_VECREG
    vst %v3,32(\CIJ_REG,LOCAL_VAR1)
    vfmdb    %v4,%v27,\ALPHA_VECREG
    vst %v4,48(\CIJ_REG,LOCAL_VAR1)
    
    vfmdb    %v1,%v28,\ALPHA_VECREG
    vst %v1,0(\CIJ_REG,LOCAL_VAR2)
    vfmdb    %v2,%v29,\ALPHA_VECREG
    vst %v2,16(\CIJ_REG,LOCAL_VAR2)
    vfmdb    %v3,%v30,\ALPHA_VECREG
    vst %v3,32(\CIJ_REG,LOCAL_VAR2)
    vfmdb    %v4,%v31,\ALPHA_VECREG
    vst %v4,48(\CIJ_REG,LOCAL_VAR2)
   la \CIJ_REG,64(\CIJ_REG)
 
.endm
/**************************************Kernel4x4*************************************************/

/*Zero C block Vectors*/
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

/*Calculate for 4x4 C blocks*/
.macro CALC_4x4  PTR_A_REG,PTR_B_REG
        vlrepg %v7, 0(\PTR_B_REG)  
        vlrepg %v1,8(\PTR_B_REG)    
        vl  %v2, 0(\PTR_A_REG)     
        vl  %v3, 16(\PTR_A_REG)         
        vfmadb    %v16,%v2,%v7,%v16   
        vfmadb    %v17,%v3,%v7,%v17       
        vfmadb    %v20,%v2,%v1,%v20   
        vfmadb    %v21,%v3,%v1,%v21 
        vlrepg %v7,16(\PTR_B_REG)      
        vlrepg %v1,24(\PTR_B_REG)     
        vfmadb    %v24,%v2,%v7,%v24   
        vfmadb    %v25,%v3,%v7,%v25    
        la \PTR_A_REG, 32(\PTR_A_REG)         
        vfmadb    %v28,%v2,%v1,%v28   
        vfmadb    %v29,%v3,%v1,%v29   
        la \PTR_B_REG, 32(\PTR_B_REG)   
.endm
 
.macro CALC_4x4_4  PTR_A_REG,PTR_B_REG
        vlrepg %v7, 0(\PTR_B_REG)  
        vlrepg %v1,8(\PTR_B_REG)    
        vl  %v2, 0(\PTR_A_REG)     
        vl  %v3, 16(\PTR_A_REG)         
        vfmadb    %v16,%v2,%v7,%v16   
        vfmadb    %v17,%v3,%v7,%v17       
        vfmadb    %v20,%v2,%v1,%v20   
        vfmadb    %v21,%v3,%v1,%v21 
        vlrepg %v7,16(\PTR_B_REG)      
        vlrepg %v1,24(\PTR_B_REG)     
        vfmadb    %v24,%v2,%v7,%v24   
        vfmadb    %v25,%v3,%v7,%v25            
        vfmadb    %v28,%v2,%v1,%v28   
        vfmadb    %v29,%v3,%v1,%v29    

        vlrepg %v7, 32(\PTR_B_REG)  
        vlrepg %v1,40(\PTR_B_REG)    
        vl  %v2, 32(\PTR_A_REG)     
        vl  %v3, 48(\PTR_A_REG)         
        vfmadb    %v16,%v2,%v7,%v16   
        vfmadb    %v17,%v3,%v7,%v17       
        vfmadb    %v20,%v2,%v1,%v20   
        vfmadb    %v21,%v3,%v1,%v21 
        vlrepg %v7,48(\PTR_B_REG)      
        vlrepg %v1,56(\PTR_B_REG)     
        vfmadb    %v24,%v2,%v7,%v24   
        vfmadb    %v25,%v3,%v7,%v25            
        vfmadb    %v28,%v2,%v1,%v28   
        vfmadb    %v29,%v3,%v1,%v29  

        vlrepg %v7, 64(\PTR_B_REG)  
        vlrepg %v1,72(\PTR_B_REG)    
        vl  %v2, 64(\PTR_A_REG)     
        vl  %v3, 80(\PTR_A_REG)         
        vfmadb    %v16,%v2,%v7,%v16   
        vfmadb    %v17,%v3,%v7,%v17       
        vfmadb    %v20,%v2,%v1,%v20   
        vfmadb    %v21,%v3,%v1,%v21 
        vlrepg %v7,80(\PTR_B_REG)      
        vlrepg %v1,88(\PTR_B_REG)     
        vfmadb    %v24,%v2,%v7,%v24   
        vfmadb    %v25,%v3,%v7,%v25            
        vfmadb    %v28,%v2,%v1,%v28   
        vfmadb    %v29,%v3,%v1,%v29  

        vlrepg %v7, 96(\PTR_B_REG)  
        vlrepg %v1,104(\PTR_B_REG)    
        vl  %v2, 96(\PTR_A_REG)     
        vl  %v3, 112(\PTR_A_REG)         
        vfmadb    %v16,%v2,%v7,%v16   
        vfmadb    %v17,%v3,%v7,%v17       
        vfmadb    %v20,%v2,%v1,%v20   
        vfmadb    %v21,%v3,%v1,%v21 
        vlrepg %v7,112(\PTR_B_REG)    
        la \PTR_A_REG, 128(\PTR_A_REG)    
        vlrepg %v1,120(\PTR_B_REG)      
        vfmadb    %v24,%v2,%v7,%v24   
        vfmadb    %v25,%v3,%v7,%v25  
        vfmadb    %v28,%v2,%v1,%v28   
        la \PTR_B_REG, 128(\PTR_B_REG) 
        vfmadb    %v29,%v3,%v1,%v29   
.endm

/*STORE C4X4*/
.macro STORE_4x4  ALPHA_VECREG,CIJ_REG , LDC_BYTE_ORIGINAL
   
    /*add LDC_BYTE_reg=LDC_BYTE_original<<1 */  
    la LOCAL_VAR1,0(\LDC_BYTE_ORIGINAL, \LDC_BYTE_ORIGINAL)
    vl %v1,0(\CIJ_REG)
    vfmadb    %v1,%v16,\ALPHA_VECREG,%v1 
    vst %v1,0(\CIJ_REG)
      
    vl %v2,16(\CIJ_REG)
    vfmadb    %v2,%v17,\ALPHA_VECREG,%v2 
    vst %v2,16(\CIJ_REG)
 
    
    la LOCAL_VAR2,0(LOCAL_VAR1,\LDC_BYTE_ORIGINAL )
    
    /*add c LDC_BYTE*/
    vl %v1,0(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vfmadb    %v1,%v20,\ALPHA_VECREG,%v1 
    vst %v1,0(\CIJ_REG,\LDC_BYTE_ORIGINAL)

    vl %v2,16(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vfmadb    %v2,%v21,\ALPHA_VECREG,%v2 
    vst %v2,16(\CIJ_REG,\LDC_BYTE_ORIGINAL)
 
    vl %v1,0(\CIJ_REG,LOCAL_VAR1)
    vfmadb    %v1,%v24,\ALPHA_VECREG,%v1 
    vst %v1,0(\CIJ_REG,LOCAL_VAR1)

    vl %v2,16(\CIJ_REG,LOCAL_VAR1)
    vfmadb    %v2,%v25,\ALPHA_VECREG,%v2 
    vst %v2,16(\CIJ_REG,LOCAL_VAR1)

 
    vl %v1,0(\CIJ_REG,LOCAL_VAR2)
    vfmadb    %v1,%v28,\ALPHA_VECREG,%v1 
    vst %v1,0(\CIJ_REG,LOCAL_VAR2)

    vl %v2,16(\CIJ_REG,LOCAL_VAR2)
    vfmadb    %v2,%v29,\ALPHA_VECREG,%v2 
    vst %v2,16(\CIJ_REG,LOCAL_VAR2) 

   la \CIJ_REG,32(\CIJ_REG)
.endm

/*STORE TRMM C4X4*/
.macro STORE_TRMM_4x4  ALPHA_VECREG,CIJ_REG , LDC_BYTE_ORIGINAL
    /*add LDC_BYTE_reg=LDC_BYTE_original<<1 */  
    la LOCAL_VAR1,0(\LDC_BYTE_ORIGINAL, \LDC_BYTE_ORIGINAL)
    vfmdb    %v1,%v16,\ALPHA_VECREG
    vst %v1,0(\CIJ_REG)
    vfmdb    %v2,%v17,\ALPHA_VECREG
    vst %v2,16(\CIJ_REG)
    la LOCAL_VAR2,0(LOCAL_VAR1,\LDC_BYTE_ORIGINAL )
    vfmdb    %v1,%v20,\ALPHA_VECREG
    vst %v1,0(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vfmdb    %v2,%v21,\ALPHA_VECREG
    vst %v2,16(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vfmdb    %v1,%v24,\ALPHA_VECREG
    vst %v1,0(\CIJ_REG,LOCAL_VAR1)
    vfmdb    %v2,%v25,\ALPHA_VECREG
    vst %v2,16(\CIJ_REG,LOCAL_VAR1)
    vfmdb    %v1,%v28,\ALPHA_VECREG
    vst %v1,0(\CIJ_REG,LOCAL_VAR2)
    vfmdb    %v2,%v29,\ALPHA_VECREG
    vst %v2,16(\CIJ_REG,LOCAL_VAR2) 
   la \CIJ_REG,32(\CIJ_REG)
.endm
/**************************************Kernel2x4*************************************************/
/*Zero C block Vectors*/
.macro ZERO_CVEC_2x4
    vzero  %v1  /*a1b1 a1b2 */
    vzero  %v2  /*a1b3 a1b4 */
    vzero  %v6  /*a2b1 a2b2 */
    vzero  %v7  /*a2b3 a2b4 */ 
.endm

/*Calculate for 2x4_4 C blocks.This Time BroadCast A. but Load B multiple*/
.macro CALC_2x4_4  PTR_A_REG,PTR_B_REG
        vl %v4, 0(\PTR_B_REG)  
        vl %v5,16(\PTR_B_REG)    
        vlrepg  %v3, 0(\PTR_A_REG) 
        vlrepg  %v16, 8(\PTR_A_REG)       
        vfmadb    %v1,%v3,%v4,%v1  
        vfmadb    %v2,%v3,%v5,%v2    
        vfmadb    %v6,%v16,%v4,%v6  
        vfmadb    %v7,%v16,%v5,%v7      

        vl %v4, 32(\PTR_B_REG)  
        vl %v5,48(\PTR_B_REG)    
        vlrepg  %v3, 16(\PTR_A_REG) 
        vlrepg  %v16, 24(\PTR_A_REG)       
        vfmadb    %v1,%v3,%v4,%v1  
        vfmadb    %v2,%v3,%v5,%v2    
        vfmadb    %v6,%v16,%v4,%v6  
        vfmadb    %v7,%v16,%v5,%v7 

        vl %v4, 64(\PTR_B_REG)  
        vl %v5,80(\PTR_B_REG)    
        vlrepg  %v3, 32(\PTR_A_REG) 
        vlrepg  %v16, 40(\PTR_A_REG)       
        vfmadb    %v1,%v3,%v4,%v1  
        vfmadb    %v2,%v3,%v5,%v2    
        vfmadb    %v6,%v16,%v4,%v6  
        vfmadb    %v7,%v16,%v5,%v7 

        vl %v4, 96(\PTR_B_REG)  
        vl %v5,112(\PTR_B_REG)    
        vlrepg  %v3, 48(\PTR_A_REG) 
        vlrepg  %v16, 56(\PTR_A_REG)       
        vfmadb    %v1,%v3,%v4,%v1  
        vfmadb    %v2,%v3,%v5,%v2  
        la \PTR_B_REG, 128(\PTR_B_REG)    
        vfmadb    %v6,%v16,%v4,%v6  
        vfmadb    %v7,%v16,%v5,%v7    
         la \PTR_A_REG, 64(\PTR_A_REG)    
.endm

/*Calculate for 2x4 C blocks.This Time BroadCast A. but Load B multiple*/
.macro CALC_2x4  PTR_A_REG,PTR_B_REG
        vl %v4, 0(\PTR_B_REG)  
        vl %v5,16(\PTR_B_REG)    
        vlrepg  %v3, 0(\PTR_A_REG) 
        vlrepg  %v16, 8(\PTR_A_REG)       
        vfmadb    %v1,%v3,%v4,%v1  
        vfmadb    %v2,%v3,%v5,%v2   
        la \PTR_A_REG, 16(\PTR_A_REG) 
        vfmadb    %v6,%v16,%v4,%v6  
        vfmadb    %v7,%v16,%v5,%v7      
        la \PTR_B_REG, 32(\PTR_B_REG)    
.endm

.macro STORE_2x4  ALPHA_REG,CIJ_REG , LDC_BYTE_ORIGINAL
/**/
    vfmdb %v1,%v1,\ALPHA_REG
    vfmdb %v2,%v2,\ALPHA_REG
    vfmdb %v6,%v6,\ALPHA_REG
    vfmdb %v7,%v7,\ALPHA_REG
    vrepg %v4,%v1,1
    vrepg %v5,%v6,1
    la LOCAL_VAR1,0(\LDC_BYTE_ORIGINAL, \LDC_BYTE_ORIGINAL) 
    adb %f1, 0(\CIJ_REG)
    std %f1,0(\CIJ_REG)

    adb %f6, 8(\CIJ_REG)
    std %f6,8(\CIJ_REG)
     
    adb %f4,0(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    std %f4,0(\CIJ_REG,\LDC_BYTE_ORIGINAL) 
    
    adb %f5,8(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    std %f5,8(\CIJ_REG,\LDC_BYTE_ORIGINAL) 
     
      /*add LDC_BYTE */
    la LOCAL_VAR2,0(LOCAL_VAR1,\LDC_BYTE_ORIGINAL )
    vrepg %v4,%v2,1
    vrepg %v5,%v7,1

    adb %f2,0(\CIJ_REG,LOCAL_VAR1)
    std %f2,0(\CIJ_REG,LOCAL_VAR1)
    
    adb %f7,8(\CIJ_REG,LOCAL_VAR1)
    std %f7,8(\CIJ_REG,LOCAL_VAR1)

    adb %f4,0(\CIJ_REG,LOCAL_VAR2)
    std %f4,0(\CIJ_REG,LOCAL_VAR2)

    adb %f5,8(\CIJ_REG,LOCAL_VAR2)
    std %f5,8(\CIJ_REG,LOCAL_VAR2)
    la \CIJ_REG,16(\CIJ_REG)

.endm

.macro STORE_TRMM_2x4  ALPHA_REG,CIJ_REG , LDC_BYTE_ORIGINAL
/**/
    vfmdb %v1,%v1,\ALPHA_REG
    vfmdb %v2,%v2,\ALPHA_REG
    vfmdb %v6,%v6,\ALPHA_REG
    vfmdb %v7,%v7,\ALPHA_REG
    vrepg %v4,%v1,1
    vrepg %v5,%v6,1
    la LOCAL_VAR1,0(\LDC_BYTE_ORIGINAL, \LDC_BYTE_ORIGINAL) 
    std %f1,0(\CIJ_REG)
    std %f6,8(\CIJ_REG)
    std %f4,0(\CIJ_REG,\LDC_BYTE_ORIGINAL) 
    std %f5,8(\CIJ_REG,\LDC_BYTE_ORIGINAL) 
      /*add LDC_BYTE */
    la LOCAL_VAR2,0(LOCAL_VAR1,\LDC_BYTE_ORIGINAL )
    vrepg %v4,%v2,1
    vrepg %v5,%v7,1
    std %f2,0(\CIJ_REG,LOCAL_VAR1)
    std %f7,8(\CIJ_REG,LOCAL_VAR1)
    std %f4,0(\CIJ_REG,LOCAL_VAR2)
    std %f5,8(\CIJ_REG,LOCAL_VAR2)
    la \CIJ_REG,16(\CIJ_REG)
.endm

/**************************************Kernel1x4*************************************************/
/*Zero C block Vectors*/
.macro ZERO_CVEC_1x4
    vzero  %v1  
    vzero  %v2       
.endm
/*Calculate for 1x4 C blocks.This Time BroadCast A. but Load B multiple*/
.macro CALC_1x4  PTR_A_REG,PTR_B_REG
        vl %v4, 0(\PTR_B_REG)  
        vl %v5,16(\PTR_B_REG)    
        vlrepg  %v3, 0(\PTR_A_REG)         
        vfmadb    %v1,%v3,%v4,%v1   
        la \PTR_A_REG, 8(\PTR_A_REG)         
        vfmadb    %v2,%v3,%v5,%v2        
        la \PTR_B_REG, 32(\PTR_B_REG)    
.endm

/*Calculate for 1x4_4 C blocks.This Time BroadCast A. but Load B multiple*/
.macro CALC_1x4_4  PTR_A_REG,PTR_B_REG
        vl %v4, 0(\PTR_B_REG)  
        vl %v5,16(\PTR_B_REG)    
        vlrepg  %v3, 0(\PTR_A_REG)         
        vfmadb    %v1,%v3,%v4,%v1           
        vfmadb    %v2,%v3,%v5,%v2  

        vl %v4, 32(\PTR_B_REG)  
        vl %v5,48(\PTR_B_REG)    
        vlrepg  %v3, 8(\PTR_A_REG)         
        vfmadb    %v1,%v3,%v4,%v1           
        vfmadb    %v2,%v3,%v5,%v2

        vl %v4, 64(\PTR_B_REG)  
        vl %v5,80(\PTR_B_REG)    
        vlrepg  %v3, 16(\PTR_A_REG)         
        vfmadb    %v1,%v3,%v4,%v1           
        vfmadb    %v2,%v3,%v5,%v2

        vl %v4, 96(\PTR_B_REG)  
        vl %v5,112(\PTR_B_REG)    
        vlrepg  %v3, 24(\PTR_A_REG)         
        vfmadb    %v1,%v3,%v4,%v1           
        vfmadb    %v2,%v3,%v5,%v2  
        la \PTR_A_REG, 32(\PTR_A_REG)       
        la \PTR_B_REG, 128(\PTR_B_REG)    
.endm

.macro STORE_1x4  ALPHA_REG,CIJ_REG , LDC_BYTE_ORIGINAL
/**/
    vfmdb %v1,%v1,\ALPHA_REG
    vfmdb %v2,%v2,\ALPHA_REG
    vrepg %v4,%v1,1
    vrepg %v5,%v2,1
    la LOCAL_VAR1,0(\LDC_BYTE_ORIGINAL, \LDC_BYTE_ORIGINAL) 
    adb %f1, 0(\CIJ_REG)
    std %f1,0(\CIJ_REG)
     
    adb %f4,0(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    std %f4,0(\CIJ_REG,\LDC_BYTE_ORIGINAL) 
      /*add LDC_BYTE */
    la LOCAL_VAR2,0(LOCAL_VAR1,\LDC_BYTE_ORIGINAL )
    adb %f2,0(\CIJ_REG,LOCAL_VAR1)
    std %f2,0(\CIJ_REG,LOCAL_VAR1)
    adb %f5,0(\CIJ_REG,LOCAL_VAR2)
    std %f5,0(\CIJ_REG,LOCAL_VAR2)
    la \CIJ_REG,8(\CIJ_REG)

.endm

.macro STORE_TRMM_1x4  ALPHA_REG,CIJ_REG , LDC_BYTE_ORIGINAL
/**/
    vfmdb %v1,%v1,\ALPHA_REG
    vfmdb %v2,%v2,\ALPHA_REG
    vrepg %v4,%v1,1
    vrepg %v5,%v2,1
    la LOCAL_VAR1,0(\LDC_BYTE_ORIGINAL, \LDC_BYTE_ORIGINAL) 
    std %f1,0(\CIJ_REG)
    std %f4,0(\CIJ_REG,\LDC_BYTE_ORIGINAL) 
      /*add LDC_BYTE */
    la LOCAL_VAR2,0(LOCAL_VAR1,\LDC_BYTE_ORIGINAL )
    std %f2,0(\CIJ_REG,LOCAL_VAR1)
    std %f5,0(\CIJ_REG,LOCAL_VAR2)
    la \CIJ_REG,8(\CIJ_REG)
.endm
/***************************************BN=2 SECTION***************************************/
/*************************************Kernel8x2***************************************************/
/*Zero C block Vectors*/
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

/*Calculate for 8x2 C blocks*/
.macro CALC_8x2  PTR_A_REG,PTR_B_REG
        vlrepg %v7, 0(\PTR_B_REG)  
        vlrepg %v1,8(\PTR_B_REG)    
        vl  %v2, 0(\PTR_A_REG)     
        vl  %v3, 16(\PTR_A_REG)     
        vl  %v4, 32(\PTR_A_REG)   
        vl  %v5, 48(\PTR_A_REG)      
        vfmadb    %v16,%v2,%v7,%v16   
        vfmadb    %v17,%v3,%v7,%v17   
        vfmadb    %v18,%v4,%v7,%v18  
        vfmadb    %v19,%v5,%v7,%v19     
        vfmadb    %v20,%v2,%v1,%v20   
        vfmadb    %v21,%v3,%v1,%v21  
        la \PTR_A_REG, 64(\PTR_A_REG)     
        vfmadb    %v22,%v4,%v1,%v22  
        vfmadb    %v23,%v5,%v1,%v23       
        la \PTR_B_REG, 16(\PTR_B_REG)    
.endm
 

/*Calculate for 8x2_4 C blocks*/
.macro CALC_8x2_4  PTR_A_REG,PTR_B_REG
        vlrepg %v7, 0(\PTR_B_REG)  
        vlrepg %v1,8(\PTR_B_REG)    
        vl  %v2, 0(\PTR_A_REG)     
        vl  %v3, 16(\PTR_A_REG)     
        vl  %v4, 32(\PTR_A_REG)   
        vl  %v5, 48(\PTR_A_REG)      
        vfmadb    %v16,%v2,%v7,%v16   
        vfmadb    %v17,%v3,%v7,%v17   
        vfmadb    %v18,%v4,%v7,%v18  
        vfmadb    %v19,%v5,%v7,%v19     
        vfmadb    %v20,%v2,%v1,%v20   
        vfmadb    %v21,%v3,%v1,%v21       
        vfmadb    %v22,%v4,%v1,%v22  
        vfmadb    %v23,%v5,%v1,%v23    

        vlrepg %v7, 16(\PTR_B_REG)  
        vlrepg %v1,24(\PTR_B_REG)    
        vl  %v2, 64(\PTR_A_REG)     
        vl  %v3, 80(\PTR_A_REG)     
        vl  %v4, 96(\PTR_A_REG)   
        vl  %v5, 112(\PTR_A_REG)      
        vfmadb    %v16,%v2,%v7,%v16   
        vfmadb    %v17,%v3,%v7,%v17   
        vfmadb    %v18,%v4,%v7,%v18  
        vfmadb    %v19,%v5,%v7,%v19     
        vfmadb    %v20,%v2,%v1,%v20   
        vfmadb    %v21,%v3,%v1,%v21       
        vfmadb    %v22,%v4,%v1,%v22  
        vfmadb    %v23,%v5,%v1,%v23 

        vlrepg %v7, 32(\PTR_B_REG)  
        vlrepg %v1,40(\PTR_B_REG)    
        vl  %v2, 128(\PTR_A_REG)     
        vl  %v3, 144(\PTR_A_REG)     
        vl  %v4, 160(\PTR_A_REG)   
        vl  %v5, 176(\PTR_A_REG)      
        vfmadb    %v16,%v2,%v7,%v16   
        vfmadb    %v17,%v3,%v7,%v17   
        vfmadb    %v18,%v4,%v7,%v18  
        vfmadb    %v19,%v5,%v7,%v19     
        vfmadb    %v20,%v2,%v1,%v20   
        vfmadb    %v21,%v3,%v1,%v21       
        vfmadb    %v22,%v4,%v1,%v22  
        vfmadb    %v23,%v5,%v1,%v23 

        vlrepg %v7, 48(\PTR_B_REG)  
        vlrepg %v1,56(\PTR_B_REG)    
        vl  %v2, 192(\PTR_A_REG)     
        vl  %v3, 208(\PTR_A_REG)     
        vl  %v4, 224(\PTR_A_REG)   
        vl  %v5, 240(\PTR_A_REG)      
        vfmadb    %v16,%v2,%v7,%v16   
        vfmadb    %v17,%v3,%v7,%v17   
        vfmadb    %v18,%v4,%v7,%v18  
        vfmadb    %v19,%v5,%v7,%v19   
        la \PTR_B_REG, 64(\PTR_B_REG)  
        vfmadb    %v20,%v2,%v1,%v20   
        vfmadb    %v21,%v3,%v1,%v21       
        vfmadb    %v22,%v4,%v1,%v22  
        vfmadb    %v23,%v5,%v1,%v23 
        la \PTR_A_REG, 256(\PTR_A_REG)
.endm

/*STORE C8X2*/
.macro STORE_8x2  ALPHA_VECREG,CIJ_REG , LDC_BYTE_ORIGINAL
    
    vl %v1,0(\CIJ_REG)
    vfmadb    %v1,%v16,\ALPHA_VECREG,%v1 
    vst %v1,0(\CIJ_REG)
      
    vl %v2,16(\CIJ_REG)
    vfmadb    %v2,%v17,\ALPHA_VECREG,%v2 
    vst %v2,16(\CIJ_REG)

    vl %v3,32(\CIJ_REG)
    vfmadb    %v3,%v18,\ALPHA_VECREG,%v3 
    vst %v3,32(\CIJ_REG)

    vl %v4,48(\CIJ_REG)
    vfmadb    %v4,%v19,\ALPHA_VECREG,%v4 
    vst %v4,48(\CIJ_REG)
     
 
    vl %v1,0(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vfmadb    %v1,%v20,\ALPHA_VECREG,%v1 
    vst %v1,0(\CIJ_REG,\LDC_BYTE_ORIGINAL)

    vl %v2,16(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vfmadb    %v2,%v21,\ALPHA_VECREG,%v2 
    vst %v2,16(\CIJ_REG,\LDC_BYTE_ORIGINAL)
 

    vl %v3,32(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vfmadb    %v3,%v22,\ALPHA_VECREG,%v3 
    vst %v3,32(\CIJ_REG,\LDC_BYTE_ORIGINAL)

    vl %v4,48(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vfmadb    %v4,%v23,\ALPHA_VECREG,%v4 
    vst %v4,48(\CIJ_REG,\LDC_BYTE_ORIGINAL)

 
   la \CIJ_REG,64(\CIJ_REG)
 
.endm

/*STORE TRMM C8X2*/
.macro STORE_TRMM_8x2  ALPHA_VECREG,CIJ_REG , LDC_BYTE_ORIGINAL
    vfmdb    %v1,%v16,\ALPHA_VECREG
    vst %v1,0(\CIJ_REG)
    vfmdb    %v2,%v17,\ALPHA_VECREG
    vst %v2,16(\CIJ_REG)
    vfmdb    %v3,%v18,\ALPHA_VECREG
    vst %v3,32(\CIJ_REG)
    vfmdb    %v4,%v19,\ALPHA_VECREG
    vst %v4,48(\CIJ_REG)
    vfmdb    %v1,%v20,\ALPHA_VECREG
    vst %v1,0(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vfmdb    %v2,%v21,\ALPHA_VECREG
    vst %v2,16(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vfmdb    %v3,%v22,\ALPHA_VECREG
    vst %v3,32(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vfmdb    %v4,%v23,\ALPHA_VECREG
    vst %v4,48(\CIJ_REG,\LDC_BYTE_ORIGINAL)
   la \CIJ_REG,64(\CIJ_REG)
.endm

/*************************************Kernel4x2***************************************************/
/*Zero C block Vectors*/
.macro ZERO_CVEC_4x2
    vzero  %v16  
    vzero  %v17     
    vzero  %v20   
    vzero  %v21    
     
.endm

/*Calculate for 4x2 C blocks*/
.macro CALC_4x2  PTR_A_REG,PTR_B_REG
        vlrepg %v7, 0(\PTR_B_REG)  
        vlrepg %v1,8(\PTR_B_REG)    
        vl  %v2, 0(\PTR_A_REG)     
        vl  %v3, 16(\PTR_A_REG)       
        vfmadb    %v16,%v2,%v7,%v16   
        vfmadb    %v17,%v3,%v7,%v17 
        la \PTR_A_REG, 32(\PTR_A_REG)       
        vfmadb    %v20,%v2,%v1,%v20   
        vfmadb    %v21,%v3,%v1,%v21   
        la \PTR_B_REG, 16(\PTR_B_REG)    
.endm

/*Calculate for 4x2_4 C blocks*/
.macro CALC_4x2_4  PTR_A_REG,PTR_B_REG

        vlrepg %v7, 0(\PTR_B_REG)  
        vlrepg %v1,8(\PTR_B_REG)    
        vl  %v2, 0(\PTR_A_REG)     
        vl  %v3, 16(\PTR_A_REG)       
        vfmadb    %v16,%v2,%v7,%v16   
        vfmadb    %v17,%v3,%v7,%v17        
        vfmadb    %v20,%v2,%v1,%v20   
        vfmadb    %v21,%v3,%v1,%v21   

        vlrepg %v7, 16(\PTR_B_REG)  
        vlrepg %v1,24(\PTR_B_REG)    
        vl  %v2, 32(\PTR_A_REG)     
        vl  %v3, 48(\PTR_A_REG)       
        vfmadb    %v16,%v2,%v7,%v16   
        vfmadb    %v17,%v3,%v7,%v17        
        vfmadb    %v20,%v2,%v1,%v20   
        vfmadb    %v21,%v3,%v1,%v21  

        vlrepg %v7, 32(\PTR_B_REG)  
        vlrepg %v1,40(\PTR_B_REG)    
        vl  %v2, 64(\PTR_A_REG)     
        vl  %v3, 80(\PTR_A_REG)       
        vfmadb    %v16,%v2,%v7,%v16   
        vfmadb    %v17,%v3,%v7,%v17        
        vfmadb    %v20,%v2,%v1,%v20   
        vfmadb    %v21,%v3,%v1,%v21  


        vlrepg %v7, 48(\PTR_B_REG)  
        vlrepg %v1,56(\PTR_B_REG)    
        vl  %v2, 96(\PTR_A_REG)     
        vl  %v3, 112(\PTR_A_REG)       
        vfmadb    %v16,%v2,%v7,%v16   
        vfmadb    %v17,%v3,%v7,%v17  
        la \PTR_B_REG, 64(\PTR_B_REG)         
        vfmadb    %v20,%v2,%v1,%v20   
        vfmadb    %v21,%v3,%v1,%v21  
        la \PTR_A_REG, 128(\PTR_A_REG)    
.endm
 

/*STORE C4x2*/
.macro STORE_4x2  ALPHA_VECREG,CIJ_REG , LDC_BYTE_ORIGINAL
    
    vl %v1,0(\CIJ_REG)
    vfmadb    %v1,%v16,\ALPHA_VECREG,%v1 
    vst %v1,0(\CIJ_REG)
      
    vl %v2,16(\CIJ_REG)
    vfmadb    %v2,%v17,\ALPHA_VECREG,%v2 
    vst %v2,16(\CIJ_REG)
 
 
    vl %v1,0(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vfmadb    %v1,%v20,\ALPHA_VECREG,%v1 
    vst %v1,0(\CIJ_REG,\LDC_BYTE_ORIGINAL)

    vl %v2,16(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vfmadb    %v2,%v21,\ALPHA_VECREG,%v2 
    vst %v2,16(\CIJ_REG,\LDC_BYTE_ORIGINAL) 
 
    la \CIJ_REG,32(\CIJ_REG)
 
.endm

/*STORE TRMM C4x2*/
.macro STORE_TRMM_4x2  ALPHA_VECREG,CIJ_REG , LDC_BYTE_ORIGINAL
    vfmdb    %v1,%v16,\ALPHA_VECREG
    vst %v1,0(\CIJ_REG)
    vfmdb    %v2,%v17,\ALPHA_VECREG
    vst %v2,16(\CIJ_REG)
    vfmdb    %v1,%v20,\ALPHA_VECREG
    vst %v1,0(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vfmdb    %v2,%v21,\ALPHA_VECREG
    vst %v2,16(\CIJ_REG,\LDC_BYTE_ORIGINAL) 
    la \CIJ_REG,32(\CIJ_REG)
.endm

/*************************************Kernel2x2***************************************************/
/*Zero C block Vectors*/
.macro ZERO_CVEC_2x2
    vzero  %v16       
    vzero  %v20      
     
.endm

/*Calculate for 2x2 C blocks*/
.macro CALC_2x2  PTR_A_REG,PTR_B_REG
        vlrepg %v7, 0(\PTR_B_REG)  
        vlrepg %v1,8(\PTR_B_REG)    
        vl  %v2, 0(\PTR_A_REG)         
        vfmadb    %v16,%v2,%v7,%v16    
        la \PTR_A_REG, 16(\PTR_A_REG)       
        vfmadb    %v20,%v2,%v1,%v20     
        la \PTR_B_REG, 16(\PTR_B_REG)    
.endm
 
/*Calculate for 2x2_4 C blocks*/
.macro CALC_2x2_4  PTR_A_REG,PTR_B_REG
        vlrepg %v7, 0(\PTR_B_REG)  
        vlrepg %v1,8(\PTR_B_REG)    
        vl  %v2, 0(\PTR_A_REG)         
        vfmadb    %v16,%v2,%v7,%v16          
        vfmadb    %v20,%v2,%v1,%v20   

        vlrepg %v7, 16(\PTR_B_REG)  
        vlrepg %v1,24(\PTR_B_REG)    
        vl  %v2, 16(\PTR_A_REG)         
        vfmadb    %v16,%v2,%v7,%v16          
        vfmadb    %v20,%v2,%v1,%v20 

        vlrepg %v7, 32(\PTR_B_REG)  
        vlrepg %v1,40(\PTR_B_REG)    
        vl  %v2, 32(\PTR_A_REG)         
        vfmadb    %v16,%v2,%v7,%v16          
        vfmadb    %v20,%v2,%v1,%v20 


        vlrepg %v7, 48(\PTR_B_REG)  
        vlrepg %v1,56(\PTR_B_REG)    
        vl  %v2, 48(\PTR_A_REG)         
        vfmadb    %v16,%v2,%v7,%v16          
        vfmadb    %v20,%v2,%v1,%v20  
  
        la \PTR_B_REG, 64(\PTR_B_REG)  
        la \PTR_A_REG, 64(\PTR_A_REG)    
.endm

/*STORE C2x2*/
.macro STORE_2x2  ALPHA_VECREG,CIJ_REG , LDC_BYTE_ORIGINAL
    
    vl %v1,0(\CIJ_REG)
    vfmadb    %v1,%v16,\ALPHA_VECREG,%v1 
    vst %v1,0(\CIJ_REG) 

    vl %v1,0(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vfmadb    %v1,%v20,\ALPHA_VECREG,%v1 
    vst %v1,0(\CIJ_REG,\LDC_BYTE_ORIGINAL) 

    la \CIJ_REG,16(\CIJ_REG)
 
.endm

/*STORE TRMM C2x2*/
.macro STORE_TRMM_2x2  ALPHA_VECREG,CIJ_REG , LDC_BYTE_ORIGINAL
    vfmdb    %v1,%v16,\ALPHA_VECREG
    vst %v1,0(\CIJ_REG) 
    vfmdb    %v1,%v20,\ALPHA_VECREG
    vst %v1,0(\CIJ_REG,\LDC_BYTE_ORIGINAL) 
    la \CIJ_REG,16(\CIJ_REG)
.endm

/**************************************Kernel1x2*************************************************/
/*Zero C block Vectors*/
.macro ZERO_CVEC_1x2
    vzero  %v1       
.endm
/*Calculate for 1x2 C blocks.This Time BroadCast A. but Load B multiple*/
.macro CALC_1x2  PTR_A_REG,PTR_B_REG
        vl %v4, 0(\PTR_B_REG)      
        vlrepg  %v3, 0(\PTR_A_REG)  
        la \PTR_B_REG, 16(\PTR_B_REG)       
        vfmadb    %v1,%v3,%v4,%v1   
        la \PTR_A_REG, 8(\PTR_A_REG)      
.endm

.macro CALC_1x2_4  PTR_A_REG,PTR_B_REG
        vl %v4, 0(\PTR_B_REG)      
        vlrepg  %v3, 0(\PTR_A_REG)         
        vfmadb    %v1,%v3,%v4,%v1  

        vl %v4, 16(\PTR_B_REG)      
        vlrepg  %v3, 8(\PTR_A_REG)         
        vfmadb    %v1,%v3,%v4,%v1  

        vl %v4, 32(\PTR_B_REG)      
        vlrepg  %v3, 16(\PTR_A_REG)         
        vfmadb    %v1,%v3,%v4,%v1  

        vl %v4, 48(\PTR_B_REG)      
        vlrepg  %v3, 24(\PTR_A_REG)         
        vfmadb    %v1,%v3,%v4,%v1  

        la \PTR_B_REG, 64(\PTR_B_REG) 
        la \PTR_A_REG, 32(\PTR_A_REG)      
.endm

.macro STORE_1x2  ALPHA_REG,CIJ_REG , LDC_BYTE_ORIGINAL
/**/
    vfmdb %v1,%v1,\ALPHA_REG 
    vrepg %v4,%v1,1  
    adb %f1, 0(\CIJ_REG)
    std %f1,0(\CIJ_REG)

    adb %f4,0(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    std %f4,0(\CIJ_REG,\LDC_BYTE_ORIGINAL) 
      
    la \CIJ_REG,8(\CIJ_REG)

.endm

.macro STORE_TRMM_1x2  ALPHA_REG,CIJ_REG , LDC_BYTE_ORIGINAL
/**/
    vfmdb %v1,%v1,\ALPHA_REG 
    vrepg %v4,%v1,1  
    std %f1,0(\CIJ_REG)
    std %f4,0(\CIJ_REG,\LDC_BYTE_ORIGINAL) 
    la \CIJ_REG,8(\CIJ_REG)
.endm

/**************************************BN=1*******************************************************/
/*************************************Kernel8x1***************************************************/
/*Zero C block Vectors*/
.macro ZERO_CVEC_8x1
    vzero  %v16  
    vzero  %v17
    vzero  %v18  
    vzero  %v19     
.endm
/*Calculate for 8x1 C blocks*/
.macro CALC_8x1  PTR_A_REG,PTR_B_REG
        vlrepg %v7, 0(\PTR_B_REG)    
        vl  %v2, 0(\PTR_A_REG)     
        vl  %v3, 16(\PTR_A_REG)     
        vl  %v4, 32(\PTR_A_REG)   
        vl  %v5, 48(\PTR_A_REG)  
        la \PTR_B_REG, 8(\PTR_B_REG)    
        vfmadb    %v16,%v2,%v7,%v16      
        vfmadb    %v17,%v3,%v7,%v17   
        vfmadb    %v18,%v4,%v7,%v18  
        la \PTR_A_REG, 64(\PTR_A_REG)  
        vfmadb    %v19,%v5,%v7,%v19    
.endm

/*Calculate for 8x1_4 C blocks*/
.macro CALC_8x1_4  PTR_A_REG,PTR_B_REG
        vlrepg %v7, 0(\PTR_B_REG)    
        vl  %v2, 0(\PTR_A_REG)     
        vl  %v3, 16(\PTR_A_REG)     
        vl  %v4, 32(\PTR_A_REG)   
        vl  %v5, 48(\PTR_A_REG)      
        vfmadb    %v16,%v2,%v7,%v16      
        vfmadb    %v17,%v3,%v7,%v17   
        vfmadb    %v18,%v4,%v7,%v18    
        vfmadb    %v19,%v5,%v7,%v19 

        vlrepg %v7, 8(\PTR_B_REG)    
        vl  %v2, 64(\PTR_A_REG)     
        vl  %v3, 80(\PTR_A_REG)     
        vl  %v4, 96(\PTR_A_REG)   
        vl  %v5, 112(\PTR_A_REG)      
        vfmadb    %v16,%v2,%v7,%v16      
        vfmadb    %v17,%v3,%v7,%v17   
        vfmadb    %v18,%v4,%v7,%v18    
        vfmadb    %v19,%v5,%v7,%v19 

        vlrepg %v7, 16(\PTR_B_REG)    
        vl  %v2, 128(\PTR_A_REG)     
        vl  %v3, 144(\PTR_A_REG)     
        vl  %v4, 160(\PTR_A_REG)   
        vl  %v5, 176(\PTR_A_REG)      
        vfmadb    %v16,%v2,%v7,%v16      
        vfmadb    %v17,%v3,%v7,%v17   
        vfmadb    %v18,%v4,%v7,%v18    
        vfmadb    %v19,%v5,%v7,%v19 

        vlrepg %v7, 24(\PTR_B_REG)    
        vl  %v2, 192(\PTR_A_REG)     
        vl  %v3, 208(\PTR_A_REG)     
        vl  %v4, 224(\PTR_A_REG)   
        vl  %v5, 240(\PTR_A_REG)      
        vfmadb    %v16,%v2,%v7,%v16      
        vfmadb    %v17,%v3,%v7,%v17   
        vfmadb    %v18,%v4,%v7,%v18    
        vfmadb    %v19,%v5,%v7,%v19 


        la \PTR_A_REG, 256(\PTR_A_REG)
        la \PTR_B_REG, 32(\PTR_B_REG)   
.endm
 
/*STORE C8X1*/
.macro STORE_8x1  ALPHA_VECREG,CIJ_REG , LDC_BYTE_ORIGINAL
    
    vl %v1,0(\CIJ_REG)
    vfmadb    %v1,%v16,\ALPHA_VECREG,%v1 
    vst %v1,0(\CIJ_REG)
      
    vl %v2,16(\CIJ_REG)
    vfmadb    %v2,%v17,\ALPHA_VECREG,%v2 
    vst %v2,16(\CIJ_REG)

    vl %v3,32(\CIJ_REG)
    vfmadb    %v3,%v18,\ALPHA_VECREG,%v3 
    vst %v3,32(\CIJ_REG)

    vl %v4,48(\CIJ_REG)
    vfmadb    %v4,%v19,\ALPHA_VECREG,%v4 
    vst %v4,48(\CIJ_REG)
     
    la \CIJ_REG,64(\CIJ_REG)
 
.endm
 
/*STORE TRMM C8X1*/
.macro STORE_TRMM_8x1  ALPHA_VECREG,CIJ_REG , LDC_BYTE_ORIGINAL
    vfmdb    %v1,%v16,\ALPHA_VECREG
    vst %v1,0(\CIJ_REG)
    vfmdb    %v2,%v17,\ALPHA_VECREG
    vst %v2,16(\CIJ_REG)
    vfmdb    %v3,%v18,\ALPHA_VECREG
    vst %v3,32(\CIJ_REG)
    vfmdb    %v4,%v19,\ALPHA_VECREG
    vst %v4,48(\CIJ_REG)
    la \CIJ_REG,64(\CIJ_REG)
.endm
 

/*************************************Kernel4x1***************************************************/
/*Zero C block Vectors*/
.macro ZERO_CVEC_4x1
    vzero  %v16  
    vzero  %v17      
.endm
/*Calculate for 4x1 C blocks*/
.macro CALC_4x1  PTR_A_REG,PTR_B_REG
        vlrepg %v7, 0(\PTR_B_REG)    
        vl  %v2, 0(\PTR_A_REG)     
        vl  %v3, 16(\PTR_A_REG)    
        la \PTR_B_REG, 8(\PTR_B_REG)    
        vfmadb    %v16,%v2,%v7,%v16      
        vfmadb    %v17,%v3,%v7,%v17   
        la \PTR_A_REG, 32(\PTR_A_REG)     
.endm
 
/*Calculate for 4x1_4 C blocks*/
.macro CALC_4x1_4  PTR_A_REG,PTR_B_REG
        vlrepg %v7, 0(\PTR_B_REG)    
        vl  %v2, 0(\PTR_A_REG)     
        vl  %v3, 16(\PTR_A_REG)       
        vfmadb    %v16,%v2,%v7,%v16      
        vfmadb    %v17,%v3,%v7,%v17  

        vlrepg %v7, 8(\PTR_B_REG)    
        vl  %v2, 32(\PTR_A_REG)     
        vl  %v3, 48(\PTR_A_REG)       
        vfmadb    %v16,%v2,%v7,%v16      
        vfmadb    %v17,%v3,%v7,%v17 

        vlrepg %v7, 16(\PTR_B_REG)    
        vl  %v2, 64(\PTR_A_REG)     
        vl  %v3, 80(\PTR_A_REG)       
        vfmadb    %v16,%v2,%v7,%v16      
        vfmadb    %v17,%v3,%v7,%v17 

        vlrepg %v7, 24(\PTR_B_REG)    
        vl  %v2, 96(\PTR_A_REG)     
        vl  %v3, 112(\PTR_A_REG)       
        vfmadb    %v16,%v2,%v7,%v16      
        vfmadb    %v17,%v3,%v7,%v17 

        la \PTR_B_REG, 32(\PTR_B_REG)  
        la \PTR_A_REG, 128(\PTR_A_REG)     
.endm

/*STORE C4X1*/
.macro STORE_4x1  ALPHA_VECREG,CIJ_REG , LDC_BYTE_ORIGINAL
    
    vl %v1,0(\CIJ_REG)
    vfmadb    %v1,%v16,\ALPHA_VECREG,%v1 
    vst %v1,0(\CIJ_REG)
      
    vl %v2,16(\CIJ_REG)
    vfmadb    %v2,%v17,\ALPHA_VECREG,%v2 
    vst %v2,16(\CIJ_REG)
 
     
    la \CIJ_REG,32(\CIJ_REG)
 
.endm

/*STORE TRMM C4X1*/
.macro STORE_TRMM_4x1  ALPHA_VECREG,CIJ_REG , LDC_BYTE_ORIGINAL
    vfmdb    %v1,%v16,\ALPHA_VECREG
    vst %v1,0(\CIJ_REG)
    vfmdb    %v2,%v17,\ALPHA_VECREG
    vst %v2,16(\CIJ_REG)
    la \CIJ_REG,32(\CIJ_REG)
.endm
/*************************************Kernel2x1***************************************************/
/*Zero C block Vectors*/
.macro ZERO_CVEC_2x1
    vzero  %v16       
.endm
/*Calculate for 2x1 C blocks*/
.macro CALC_2x1  PTR_A_REG,PTR_B_REG
        vlrepg %v7, 0(\PTR_B_REG)    
        vl  %v2, 0(\PTR_A_REG)      
        la \PTR_B_REG, 8(\PTR_B_REG)    
        vfmadb    %v16,%v2,%v7,%v16    
        la \PTR_A_REG, 16(\PTR_A_REG)     
.endm

/*Calculate for 2x1_4 C blocks*/
.macro CALC_2x1_4  PTR_A_REG,PTR_B_REG
        vlrepg %v7, 0(\PTR_B_REG)    
        vl  %v2, 0(\PTR_A_REG)         
        vfmadb    %v16,%v2,%v7,%v16  

        vlrepg %v7, 8(\PTR_B_REG)    
        vl  %v2, 16(\PTR_A_REG)         
        vfmadb    %v16,%v2,%v7,%v16  

        vlrepg %v7, 16(\PTR_B_REG)    
        vl  %v2, 32(\PTR_A_REG)         
        vfmadb    %v16,%v2,%v7,%v16          

        vlrepg %v7, 24(\PTR_B_REG)    
        vl  %v2, 48(\PTR_A_REG)         
        vfmadb    %v16,%v2,%v7,%v16  

        la \PTR_B_REG, 32(\PTR_B_REG)   
        la \PTR_A_REG, 64(\PTR_A_REG)     
.endm
 
/*STORE C2X1*/
.macro STORE_2x1  ALPHA_VECREG,CIJ_REG , LDC_BYTE_ORIGINAL
    
    vl %v1,0(\CIJ_REG)
    vfmadb    %v1,%v16,\ALPHA_VECREG,%v1 
    vst %v1,0(\CIJ_REG) 

    la \CIJ_REG,16(\CIJ_REG)
 
.endm

/*STORE TRMM C2X1*/
.macro STORE_TRMM_2x1  ALPHA_VECREG,CIJ_REG , LDC_BYTE_ORIGINAL
    vfmdb    %v1,%v16,\ALPHA_VECREG  
    vst %v1,0(\CIJ_REG) 
    la \CIJ_REG,16(\CIJ_REG)
.endm
/*************************************Kernel1x1***************************************************/
/*Zero C block Vectors*/
.macro ZERO_CVEC_1x1
    LZDR %f1       
.endm
/*Calculate for 1x1 C blocks*/
.macro CALC_1x1  PTR_A_REG,PTR_B_REG
     ld %f2,0(\PTR_A_REG) /**a*/
     la  \PTR_A_REG,8(\PTR_A_REG)
     madb %f1,%f2,0(\PTR_B_REG)
     la  \PTR_B_REG,8(\PTR_B_REG)     
.endm

/*Calculate for 1x1_4 C blocks*/
.macro CALC_1x1_4  PTR_A_REG,PTR_B_REG
     ld %f2,0(\PTR_A_REG) /**a*/
     madb %f1,%f2,0(\PTR_B_REG)

     ld %f2,8(\PTR_A_REG) /**a*/
     madb %f1,%f2,8(\PTR_B_REG)

     ld %f2,16(\PTR_A_REG) /**a*/
     madb %f1,%f2,16(\PTR_B_REG)

     ld %f2,24(\PTR_A_REG) /**a*/
     madb %f1,%f2,24(\PTR_B_REG)          

     la  \PTR_A_REG,32(\PTR_A_REG)
     la  \PTR_B_REG,32(\PTR_B_REG)     
.endm
 
/*STORE C1X1*/
.macro STORE_1x1  ALPHA_FLOAT,CIJ_REG,LDC_BYTE_ORIGINAL  
     ld %f2,0(CIJ_LOCAL)
     madbr %f2,%f1,\ALPHA_FLOAT
     std %f2,0(CIJ_LOCAL) 
     la \CIJ_REG,8(\CIJ_REG) 
.endm

/*STORE C1X1*/
.macro STORE_TRMM_1x1  ALPHA_FLOAT,CIJ_REG,LDC_BYTE_ORIGINAL  
     mdbr %f1,\ALPHA_FLOAT
     std %f1,0(CIJ_LOCAL) 
     la \CIJ_REG,8(\CIJ_REG) 
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
			sllg \PTR_B, \OFF_VAL,5
			la \PTR_A,0(\PTR_A,\PTR_B) /*ptrba+off*4*/
			agr \PTR_A,\PTR_B /*ptrba+off*4**/
			la \PTR_B,0(\B_VAL,\PTR_B)	  
		.elseif \C_A==4
			sllg \PTR_B, \OFF_VAL,5 
			agr \PTR_A,\PTR_B /*ptrba+off*4**/
			la \PTR_B,0(\B_VAL,\PTR_B)	  /*refresh BPOINT*/
		.elseif \C_A==2
			sllg \PTR_B, \OFF_VAL,4 
			la \PTR_A,0(\PTR_A,\PTR_B) /*ptrba+off*2**/
			agr \PTR_B, \PTR_B   
			la \PTR_B,0(\B_VAL,\PTR_B)	  /*refresh BPOINT*/

		.elseif \C_A==1
			sllg \PTR_B, \OFF_VAL,3 
			agr \PTR_A,\PTR_B /*ptrba+off*4**/
			sllg \PTR_B, \OFF_VAL,5   
			la \PTR_B,0(\B_VAL,\PTR_B)	  /*refresh BPOINT*/
		.endif

.elseif \C_B==2
		.if \C_A==8 
			sllg \PTR_B, \OFF_VAL,6 
			agr \PTR_A,\PTR_B /*ptrba+off*8**/
			sllg \PTR_B, \OFF_VAL,4   
			la \PTR_B,0(\B_VAL,\PTR_B)	  /*refresh BPOINT*/
		.elseif \C_A==4
			sllg \PTR_B, \OFF_VAL,4 
			la \PTR_A,0(\PTR_A,\PTR_B) /*ptrba+off*2**/
			agr \PTR_A,\PTR_B /*ptrba+off*2**/  
			la \PTR_B,0(\B_VAL,\PTR_B)	  /*refresh BPOINT*/
		.elseif \C_A==2
			sllg \PTR_B, \OFF_VAL,4 
			agr \PTR_A,\PTR_B /*ptrba+off*2**/ 
			la \PTR_B,0(\B_VAL,\PTR_B)	  /*refresh BPOINT*/ 
		.elseif \C_A==1
			sllg \PTR_B, \OFF_VAL,3 
			la \PTR_A,0(\PTR_A,\PTR_B) /*ptrba+off*1**/
			agr \PTR_B,\PTR_B /* off+off**/  
			la \PTR_B,0(\B_VAL,\PTR_B)	  /*refresh BPOINT*/
		.endif

.elseif \C_B==1
		.if \C_A==8 
			sllg \PTR_B, \OFF_VAL,6 
			agr \PTR_A,\PTR_B /*ptrba+off*8**/
			sllg \PTR_B, \OFF_VAL,3 
			la \PTR_B,0(\B_VAL,\PTR_B)	  /*refresh BPOINT*/
		.elseif \C_A==4
			sllg \PTR_B, \OFF_VAL,5 
			agr \PTR_A,\PTR_B /*ptrba+off*4**/
			sllg \PTR_B, \OFF_VAL,3 
			la \PTR_B,0(\B_VAL,\PTR_B)	  /*refresh BPOINT*/
		.elseif \C_A==2
			sllg \PTR_B, \OFF_VAL,3 
			la \PTR_A,0(\PTR_A,\PTR_B) /*ptrba+off*1**/
			agr \PTR_A,\PTR_B /*ptrba+off*1**/
			la \PTR_B,0(\B_VAL,\PTR_B)	  /*refresh BPOINT*/

		.elseif \C_A==1
			sllg \PTR_B, \OFF_VAL,3 
			agr \PTR_A,\PTR_B /*ptrba+off*1**/ 
			la \PTR_B,0(\B_VAL,\PTR_B)	  /*refresh BPOINT*/
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
                            /* temp = off+INCR_A;	// number of values in A */
           la \TEMP_VAL,\INCR_A(\OFF_VAL)
    #else
                            /* temp = off+INCR_B	// number of values in B*/
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
			sllg \TEMP_VAL, \TEMP_VAL,6	
		.elseif \C_A==4
			sllg \TEMP_VAL, \TEMP_VAL,5 /*temp*4*/ 
		.elseif \C_A==2
			sllg \TEMP_VAL, \TEMP_VAL,4 /*temp*2*/  
		.elseif \C_A==1
			sllg \TEMP_VAL, \TEMP_VAL,3 /*temp*1*/  
		.endif
                la \PTR_A,0(\PTR_A,\TEMP_VAL) /*ptrba+temp*C_A*/ 
                /*we do not need to refresh ptrbb. so lets ignore it*/

    #endif

    #ifdef LEFT
                    /*off += 8; // number of values in A*/
                    aghi \OFF_VAL,\C_A
    #endif
.endm