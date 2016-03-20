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
	beq $t2, $0, look
	beq $t2, $t3, stop_wait
	add $t7, $t7, 1
	j stillhere
	
	
	

