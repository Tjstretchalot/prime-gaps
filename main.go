package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"math"
	"math/big"
	"os"
	"runtime"
	"strconv"
	"time"
)

// The information we are creating.
type PrimeGapsInfo struct {
	// LastPrime is the last prime number that we saw.
	LastPrime *big.Int

	// CurrentNumber is strictly larger than LastPrime and
	// is the next number to check if it is prime. All numbers
	// between LastPrime and CurrentNumber, excluding CurrentNumber,
	// must be (probably) composite.
	CurrentNumber *big.Int

	// The counters for gaps. Index 0 should always be empty. Index 1
	// is the number of prime number gaps of distance 2, e.g., 3 and 5.
	// Note that we should always start CurrentNumber at 3 or higher.
	GapCounter []uint64

	// The number of primes we've encountered so far.
	PrimesSoFar uint64

	// MillerRabinSeeds should be negative to check for primes deterministically,
	// and any non-negative number to check for primes probabilistically using
	// the Miller-Rabin test with the given value for n and a Baillie-PSW test.
	// More bases reduces the number of false positives. Note that even with a
	// value of 0, this is perfectly accurate for CurrentNumber below 2^64.
	MillerRabinBases int

	// Only used when checking primes deterministically, such as when parallelizing
	// and determinism is important, or when MillerRabinBases is 0. The precomputed
	// prime numbers to speed up the deterministic prime check, starting with index
	// 0 = 2, index 1 = 3.
	//
	// First 10 million primes is a good target for large sweeps. You can google
	// "10 millionth prime", square it, and that's approximately how large a number
	// whose primality check is improved using the precomputed primes.
	PrecomputedPrimes []uint32
}

// PrecomputePrimes ensures that we have precomputed at least the given number of
// primes. Only works for primes below 2^32. 10 million is a good number
// Precompute these small primes for faster checks on larger primes.
func (i *PrimeGapsInfo) PrecomputePrimes(numberOfPrimes int) {
	var tmp *big.Int
	if i.PrecomputedPrimes == nil {
		i.PrecomputedPrimes = make([]uint32, 0, numberOfPrimes)
		tmp = big.NewInt(2)
	} else {
		tmp = big.NewInt(int64(i.PrecomputedPrimes[len(i.PrecomputedPrimes)-1] + 2))
	}

	one := big.NewInt(1)
	lastPrintedProgress := time.Now()

	log.Println("Precomputing primes...")
	for len(i.PrecomputedPrimes) < numberOfPrimes {
		if tmp.ProbablyPrime(0) { // deterministic for numbers this small
			i.PrecomputedPrimes = append(i.PrecomputedPrimes, uint32(tmp.Int64()))
		}
		tmp.Add(tmp, one)

		if time.Since(lastPrintedProgress) > 5*time.Second {
			log.Printf("Precomputing primes... %d", len(i.PrecomputedPrimes))
			lastPrintedProgress = time.Now()
		}
	}
	log.Printf("Finished precomputing the first %d primes", len(i.PrecomputedPrimes))
}

func (i *PrimeGapsInfo) IterateTo(targetNumberOfPrimes uint64) {
	two := big.NewInt(2)
	gapBig := big.NewInt(0)
	lastPrint := time.Now()
	var gapIndex int

	if i.MillerRabinBases >= 0 {
		for i.PrimesSoFar < targetNumberOfPrimes {
			if i.CurrentNumber.ProbablyPrime(i.MillerRabinBases) {
				gapBig.Neg(i.LastPrime)
				gapBig.Add(gapBig, i.CurrentNumber)
				gapIndex = int(gapBig.Uint64()) / 2
				for gapIndex >= len(i.GapCounter) {
					i.ExpandGapCounter()
				}
				i.GapCounter[gapIndex]++
				i.PrimesSoFar++
				i.LastPrime.Set(i.CurrentNumber)

				if time.Since(lastPrint) > time.Second*30 {
					log.Printf("Calculated %d primes so far..", i.PrimesSoFar)
					lastPrint = time.Now()
				}
			}

			i.CurrentNumber.Add(i.CurrentNumber, two)
		}
	} else {
		space := make([]big.Int, 3)
		zero := big.NewInt(0)
		for i.PrimesSoFar < targetNumberOfPrimes {
			if deterministicIsPrime(i.CurrentNumber, space, i.PrecomputedPrimes, zero) {
				gapBig.Neg(i.LastPrime)
				gapBig.Add(gapBig, i.CurrentNumber)
				gapIndex = int(gapBig.Uint64()) / 2
				for gapIndex >= len(i.GapCounter) {
					i.ExpandGapCounter()
				}
				i.GapCounter[gapIndex]++
				i.PrimesSoFar++
				i.LastPrime.Set(i.CurrentNumber)

				if time.Since(lastPrint) > time.Second*30 {
					log.Printf("Calculated %d primes so far..", i.PrimesSoFar)
					lastPrint = time.Now()
				}
			}

			i.CurrentNumber.Add(i.CurrentNumber, two)
		}
	}
}

func (i *PrimeGapsInfo) IterateToNumber(targetNumber *big.Int) {
	two := big.NewInt(2)
	gapBig := big.NewInt(0)
	lastPrint := time.Now()
	var gapIndex int

	if i.MillerRabinBases >= 0 {
		for i.CurrentNumber.Cmp(targetNumber) < 0 {
			if i.CurrentNumber.ProbablyPrime(i.MillerRabinBases) {
				gapBig.Neg(i.LastPrime)
				gapBig.Add(gapBig, i.CurrentNumber)
				gapIndex = int(gapBig.Uint64()) / 2
				for gapIndex >= len(i.GapCounter) {
					i.ExpandGapCounter()
				}
				i.GapCounter[gapIndex]++
				i.PrimesSoFar++
				i.LastPrime.Set(i.CurrentNumber)

				if time.Since(lastPrint) > time.Second*30 {
					log.Printf("Calculated %d primes so far..", i.PrimesSoFar)
					lastPrint = time.Now()
				}
			}

			i.CurrentNumber.Add(i.CurrentNumber, two)
		}
	} else {
		space := make([]big.Int, 3)
		zero := big.NewInt(0)
		for i.CurrentNumber.Cmp(targetNumber) < 0 {
			if deterministicIsPrime(i.CurrentNumber, space, i.PrecomputedPrimes, zero) {
				gapBig.Neg(i.LastPrime)
				gapBig.Add(gapBig, i.CurrentNumber)
				gapIndex = int(gapBig.Uint64()) / 2
				for gapIndex >= len(i.GapCounter) {
					i.ExpandGapCounter()
				}
				i.GapCounter[gapIndex]++
				i.PrimesSoFar++
				i.LastPrime.Set(i.CurrentNumber)

				if time.Since(lastPrint) > time.Second*30 {
					log.Printf("Calculated %d primes so far..", i.PrimesSoFar)
					lastPrint = time.Now()
				}
			}

			i.CurrentNumber.Add(i.CurrentNumber, two)
		}
	}
}

func (i *PrimeGapsInfo) IterateToParallel(targetNumberOfPrimes uint64, parallelism int, blockSize uint64) {
	if targetNumberOfPrimes <= i.PrimesSoFar {
		return
	}

	if blockSize%2 != 0 {
		log.Fatalf("blockSize must be even")
	}

	blockSizeBig := big.NewInt(int64(blockSize))
	for {
		blocks := 1

		expectedNumberOfPrimesAdjustment := i.PrimesSoFar - approxPrimesBelow(i.CurrentNumber)

		endNumber := big.NewInt(0)
		endNumber.Add(i.CurrentNumber, blockSizeBig)

		endNumberIfOneMoreBlock := big.NewInt(0)
		endNumberIfOneMoreBlock.Add(endNumber, blockSizeBig)

		expectedNumberOfPrimes := approxPrimesBelow(endNumber) + expectedNumberOfPrimesAdjustment
		expectedNumberOfPrimesIfOneMoreBlock := approxPrimesBelow(endNumberIfOneMoreBlock) + expectedNumberOfPrimesAdjustment

		for expectedNumberOfPrimesIfOneMoreBlock < targetNumberOfPrimes-50_000 {
			blocks++
			endNumber.Set(endNumberIfOneMoreBlock)
			expectedNumberOfPrimes = expectedNumberOfPrimesIfOneMoreBlock

			endNumberIfOneMoreBlock.Add(endNumberIfOneMoreBlock, blockSizeBig)
			expectedNumberOfPrimesIfOneMoreBlock = approxPrimesBelow(endNumberIfOneMoreBlock) + expectedNumberOfPrimesAdjustment

			if blocks >= parallelism {
				break
			}
		}

		if blocks < 2 {
			break
		}

		if blocks > parallelism {
			blocks = parallelism
		}
		log.Printf(
			"Running %d blocks of size %d to go from %d primes to about %d",
			blocks, blockSize, i.PrimesSoFar, expectedNumberOfPrimes,
		)

		channels := make([]chan uint64, blocks)
		for j := 0; j < blocks; j++ {
			channels[j] = make(chan uint64)
		}

		finalInfoChannel := make(chan big.Int)

		for j := 0; j < blocks; j++ {
			jthBlockStartsAt := big.NewInt(0)
			jthBlockStartsAt.Add(i.CurrentNumber, big.NewInt(int64(blockSize)*int64(j)))

			jthBlockEndsAt := big.NewInt(0)
			jthBlockEndsAt.Add(jthBlockStartsAt, big.NewInt(int64(blockSize)))

			incrementUntilDeterministicallyPrime(jthBlockStartsAt, i.PrecomputedPrimes)
			incrementUntilDeterministicallyPrime(jthBlockEndsAt, i.PrecomputedPrimes)

			go func(blockIndex int, blockStartsAt *big.Int, blockEndsAt *big.Int) {
				myInfo := PrimeGapsInfo{
					LastPrime:         blockStartsAt,
					CurrentNumber:     big.NewInt(0).Add(blockStartsAt, big.NewInt(2)),
					GapCounter:        make([]uint64, len(i.GapCounter)),
					PrimesSoFar:       1,
					MillerRabinBases:  i.MillerRabinBases,
					PrecomputedPrimes: i.PrecomputedPrimes,
				}
				myInfo.IterateToNumber(blockEndsAt)
				for k := 0; k < len(myInfo.GapCounter); k++ {
					channels[blockIndex] <- myInfo.GapCounter[k]
				}
				channels[blockIndex] <- math.MaxUint64
				channels[blockIndex] <- myInfo.PrimesSoFar

				if blockIndex == blocks-1 {
					finalInfoChannel <- *myInfo.LastPrime
					finalInfoChannel <- *myInfo.CurrentNumber
				}
			}(j, jthBlockStartsAt, jthBlockEndsAt)
		}

		for j := 0; j < blocks; j++ {
			nextGapValue := <-channels[j]
			for k := 0; nextGapValue != math.MaxUint64; k++ {
				if k >= len(i.GapCounter) {
					i.ExpandGapCounter()
				}
				i.GapCounter[k] += nextGapValue
				nextGapValue = <-channels[j]
			}
			i.PrimesSoFar += <-channels[j]
		}

		i.LastPrime = big.NewInt(0)
		i.CurrentNumber = big.NewInt(0)
		*i.LastPrime = <-finalInfoChannel
		*i.CurrentNumber = <-finalInfoChannel

		log.Printf(
			"After running blocks, now have %d primes (last prime: %s, current number: %s)",
			i.PrimesSoFar, i.LastPrime.Text(10), i.CurrentNumber.Text(10),
		)
	}

	log.Printf(
		"Finding the remaining primes (at %d, want %d) serially",
		i.PrimesSoFar, targetNumberOfPrimes,
	)
	i.IterateTo(targetNumberOfPrimes)
}

func approxPrimesBelow(n *big.Int) uint64 {
	// Prime Number Theorem: number of primes below x ~= x/ln(x).

	approxN, _ := big.NewFloat(0).SetInt(n).Float64()
	approxLogN := int64(math.Log(approxN))

	result := big.NewInt(0).Set(n)
	result.Div(result, big.NewInt(approxLogN))
	return result.Uint64()
}

func (i *PrimeGapsInfo) ExpandGapCounter() {
	newGapCounter := make([]uint64, len(i.GapCounter)*2)
	for j := 0; j < len(i.GapCounter); j++ {
		newGapCounter[j] = i.GapCounter[j]
	}
	i.GapCounter = newGapCounter
}

var precomputeWarning bool = false

func deterministicIsPrime(n *big.Int, space []big.Int, precomputedPrimes []uint32, zero *big.Int) bool {
	(&space[0]).Sqrt(n) // space[0] = stopping point

	for precomputedPrimesIndex := 0; precomputedPrimesIndex < len(precomputedPrimes); precomputedPrimesIndex++ {
		(&space[1]).SetUint64(uint64(precomputedPrimes[precomputedPrimesIndex])) // space[1] = current prime
		if (&space[2]).Rem(n, &space[1]).Cmp(zero) == 0 {                        // (space[2] = (n % space[1])) == 0
			return false
		}
		if (&space[1]).Cmp(&space[0]) > 0 {
			return true
		}
	}

	// fallback, no more precomputed primes :(
	if !precomputeWarning {
		log.Printf("Ran out of precomputed primes checking if %s is prime deterministically", n.Text(10))
		precomputeWarning = true
	}

	two := big.NewInt(2)
	for (&space[1]).Cmp(&space[0]) <= 0 {
		(&space[1]).Add(&space[1], two)
		if (&space[2]).Rem(n, &space[1]).Cmp(zero) == 0 {
			return false
		}
	}

	return true
}

func incrementUntilDeterministicallyPrime(n *big.Int, precomputedPrimes []uint32) {
	zero := big.NewInt(0)
	one := big.NewInt(1)
	space := make([]big.Int, 3)

	for !deterministicIsPrime(n, space, precomputedPrimes, zero) {
		n.Add(n, one)
	}
}

func getIntEnviron(envName string, def int) int {
	envVal, found := os.LookupEnv(envName)
	if !found {
		log.Printf("Missing environment variable %s, assuming %d", envName, def)
		return def
	}

	parsed, err := strconv.Atoi(envVal)
	if err != nil {
		log.Fatalf("Error interpreting environment variable %s: %s", envName, err)
	}
	return parsed
}

func main() {
	info := &PrimeGapsInfo{
		LastPrime:        big.NewInt(3),
		CurrentNumber:    big.NewInt(5),
		GapCounter:       make([]uint64, 512),
		PrimesSoFar:      1,
		MillerRabinBases: 10,
	}

	var err error
	var marshalled []byte
	marshalled, err = ioutil.ReadFile("info.json")
	if err == nil {
		err = json.Unmarshal(marshalled, &info)
		if err != nil {
			log.Fatalf("error unmarshalling info.json: %s", err)
		}
	} else if !os.IsNotExist(err) {
		log.Fatalf("error opening info.json: %s", err)
	}

	targetNumberOfPrimesToPrecompute := getIntEnviron("PRECOMPUTE_PRIMES", 1_000_000)
	targetNumberOfPrimesForPlot := getIntEnviron("TARGET_PRIMES", int(info.PrimesSoFar)+10_000_000)
	parallelism := getIntEnviron("PARALLELISM", runtime.NumCPU())
	blockSize := getIntEnviron("BLOCK_SIZE", 1_000_000)

	info.PrecomputePrimes(targetNumberOfPrimesToPrecompute)

	log.Printf("Continuing from %d primes...", info.PrimesSoFar)
	info.IterateToParallel(uint64(targetNumberOfPrimesForPlot), parallelism, uint64(blockSize))
	log.Printf("Now at %d primes", info.PrimesSoFar)

	marshalled, err = json.Marshal(info)
	if err != nil {
		log.Fatalf("error marshalling result: %s", err)
	}

	err = ioutil.WriteFile("info.json", marshalled, 0644)
	if err != nil {
		log.Fatalf("error closing file: %s", err)
	}
}
