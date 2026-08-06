// Package assembler builds transactions from storage's create-action results,
// wiring in provided inputs, BRC29 (un)locking scripts and change outputs, and
// exposes the assembled transaction together with the BEEF needed to sign and
// broadcast it.
package assembler
