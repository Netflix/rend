// Copyright (C) 2014 Space Monkey, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

TEXT ·monotime(SB),7,$16
	MOVQ	runtime·__vdso_clock_gettime_sym(SB), AX
	CMPQ	AX, $0
	JEQ	vdso_is_sad
	MOVL	$4, DI  // CLOCK_MONOTONIC_RAW
	LEAQ	0(SP), SI
	CALL	AX
	MOVQ	0(SP), AX
	MOVQ	8(SP), DX
	MOVQ	AX, sec+0(FP)
	MOVL	DX, nsec+8(FP)
	RET
vdso_is_sad:
	MOVQ	$0, sec+0(FP)
	RET