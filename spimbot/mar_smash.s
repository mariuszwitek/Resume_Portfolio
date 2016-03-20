# syscall constants
PRINT_STRING	= 4

# spimbot constants
VELOCITY	= 0xffff0010
ANGLE		= 0xffff0014
ANGLE_CONTROL	= 0xffff0018
BOT_X		= 0xffff0020
BOT_Y		= 0xffff0024

BONK_MASK	= 0x1000
BONK_ACK	= 0xffff0060

TIMER		= 0xffff001c
TIMER_MASK	= 0x8000
TIMER_ACK	= 0xffff006c

# fruit constants
FRUIT_SCAN	= 0xffff005c
FRUIT_SMASH	= 0xffff0068

SMOOSHED_MASK	= 0x2000
SMOOSHED_ACK	= 0xffff0064

# could be useful for debugging
PRINT_INT	= 0xffff0080


smoosh_count: .word 0
.data 
.align 2
fruit_data: .space 260

.text
main:
	li	$t4, TIMER_MASK		# timer interrupt enable bit
	or	$t4, $t4, BONK_MASK	# bonk interrupt bit
	or	$t4, $t4, SMOOSHED_MASK	# bonk interrupt bit
	or	$t4, $t4, 1		# global interrupt enable
	mtc0	$t4, $12		# set interrupt mask (Status register)
	j position

position:
	li $t1, 1
	sw $t1, ANGLE_CONTROL
	li $t1, 90
	sw $t1, ANGLE
	li $t1, 10
	sw $t1, VELOCITY
	lw $t1, BOT_Y
	li $t2, 270
	bge $t1, $t2, stop_look
	j position
	
stop_look:
	li $t1, 0 
	sw $t1, VELOCITY
	li $t1, 1
	sw $t1, ANGLE_CONTROL
	li $t1, 0
	sw $t1, ANGLE

look:
	la $t0, fruit_data
	sw $t0, FRUIT_SCAN
	li $t2, 0

locate:
	add $t2, $t2, $t0 #right?
	lw $t3, 0($t2)
	beq $t3, $0, locate
	lw $t4, 8($t2)
	lw $t1, BOT_X
	sub $t5, $t1, $t4
	li $t6, 10
	blt $t5, $0, fLocate
	li $t6, -10

fLocate:
	li $t1, 1
	sw $t1, ANGLE_CONTROL
	li $t1, 0
	sw $t1, ANGLE
	sw $t6, VELOCITY
	j move4

move4:
	lw $t1, BOT_X
	beq $t1, $t4, stop_wait
	j move4

stop_wait:
	li $t1, 0
	sw $t1, VELOCITY
	la $t0, fruit_data
	sw $t0, FRUIT_SCAN
	li $t7, 0
	j stillhere

stillhere:
	mul $t2, $t7, 16
	add $t2, $t2, $t0 
	lw $t2, 0($t2)
	# check count
	la $t1, smoosh_count
	lw $t1, 0($t1)
	li $t8, 0
	bge $t1, 5, smashme
	beq $t2, $0, look
	beq $t2, $t3, stop_wait
	add $t7, $t7, 1
	j stillhere

smashme:
	li $t1, 1
	sw $t1, ANGLE_CONTROL
	li $t1, 90
	sw $t1, ANGLE
	li $t2, 10
	sw $t1, VELOCITY
	j pos_again

pos_again:
	li $t1, 1
	sw $t1, ANGLE_CONTROL
	li $t1, 90
	sw $t1, ANGLE
	lw $t1, BOT_Y
	li $t2, 270
	blt $t1, $t2, stop_look
	j pos_again
	
.kdata				# interrupt handler data (separated just for readability)
chunkIH:	.space 8	# space for two registers
non_intrpt_str:	.asciiz "Non-interrupt exception\n"
unhandled_str:	.asciiz "Unhandled interrupt type\n"
	
.ktext 0x80000180
interrupt_handler:
.set noat
	move	$k1, $at		# Save $at                               
.set at
	la	$k0, chunkIH
	sw	$a0, 0($k0)		# Get some free registers                  
	sw	$a1, 4($k0)		# by storing them to a global variable     

	mfc0	$k0, $13		# Get Cause register                       
	srl	$a0, $k0, 2                
	and	$a0, $a0, 0xf		# ExcCode field                            
	bne	$a0, 0, non_intrpt         

interrupt_dispatch:			# Interrupt:                             
	mfc0	$k0, $13		# Get Cause register, again                 
	beq	$k0, 0, done		# handled all outstanding interrupts     

	and	$a0, $k0, BONK_MASK	# is there a bonk interrupt?                
	bne	$a0, 0, bonk_interrupt   

	and	$a0, $k0, TIMER_MASK	# is there a timer interrupt?
	bne	$a0, 0, timer_interrupt
	
	and	$a0, $k0, SMOOSHED_MASK
	bne	$a0, 0, smoosh_interrupt

	# add dispatch for other interrupt types here.

	li	$v0, PRINT_STRING	# Unhandled interrupt types
	la	$a0, unhandled_str
	syscall 
	j	done

bonk_interrupt:
	li $t1, 1
	sw $t1, FRUIT_SMASH
	sw $t1, FRUIT_SMASH
	sw $t1, FRUIT_SMASH
	sw $t1, FRUIT_SMASH
	sw $t1, FRUIT_SMASH
	
	la $t1, smoosh_count
	sw $zero, 0($t1)
	
	sw	$a1, BONK_ACK		# acknowledge interrupt
	li $t1, -10
	sw	$t1, VELOCITY		# ???

	j	interrupt_dispatch	# see if other interrupts are waiting

timer_interrupt:
	sw	$a1, TIMER_ACK		# acknowledge interrupt

	#li	$t0, 90			# ???
	#sw	$t0, ANGLE		# ???
	#sw	$zero, ANGLE_CONTROL	# ???

	lw	$v0, TIMER		# current time
	add	$v0, $v0, 50000  
	sw	$v0, TIMER		# request timer in 50000 cycles

	j	interrupt_dispatch	# see if other interrupts are waiting

smoosh_interrupt:
	sw	$a1, SMOOSHED_ACK		# acknowledge interrupt
	la $t8, smoosh_count
	lw $t9, 0($t8)
	add $t9, $t9, 1
	sw $t9, 0($t8)
	j	interrupt_dispatch	# see if other interrupts are waiting

non_intrpt:				# was some non-interrupt
	li	$v0, PRINT_STRING
	la	$a0, non_intrpt_str
	syscall				# print out an error message
	# fall through to done

done:
	la	$k0, chunkIH
	lw	$a0, 0($k0)		# Restore saved registers
	lw	$a1, 4($k0)
.set noat
	move	$at, $k1		# Restore $at
.set at 
	eret
	
	
	

