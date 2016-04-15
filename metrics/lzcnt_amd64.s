// Copyright 2015 Netflix, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//+build amd64

// #Function copied from Damian Gryski's go-bits package
// #Copyright 2015 Damian Gryski, under MIT license
// #See the NOTICE file for more details.
// #https://github.com/dgryski/go-bits

// func lzcnt(x uint64) uint64
TEXT ·lzcnt(SB),4,$0-16
        BSRQ  x+0(FP), AX
        JZ zero
        SUBQ  $63, AX
        NEGQ AX
        MOVQ AX, ret+8(FP)
        RET
zero:
        MOVQ $64, ret+8(FP)
        RET
