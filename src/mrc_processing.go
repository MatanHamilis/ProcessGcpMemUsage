package main

type mrc struct {
	AppID      uint64
	Size       uint64
	Resolution uint64
	Hist       map[uint64]uint64
}

func loadMrc(path string) map[int]*mrc {
	file := safeFileOpen(path)
	mrcs := decodeMrcJSON(file)
	return mrcs
}

func (m *mrc) getTotalSamplesInHistogram() uint64 {
	totalHist := uint64(0)
	for _, v := range m.Hist {
		totalHist += v
	}
	return totalHist
}

func (m *mrc) getMissRatioFromNormalizedMissRatio(missRatio float32) float32 {
	totalHist := m.getTotalSamplesInHistogram()
	bestAchievableMissRatio := float32(m.Hist[m.Resolution]) / float32(totalHist)
	return bestAchievableMissRatio + (1.0-bestAchievableMissRatio)*missRatio
}

func (m *mrc) getMinimalPercentageForNormalizedMissRatio(missRatio float32) float32 {
	return m.getMinimalPercentageForMissRatio(m.getMissRatioFromNormalizedMissRatio(missRatio))
}

func (m *mrc) getMinimalPercentageForMissRatio(missRatio float32) float32 {
	totalHist := m.getTotalSamplesInHistogram()
	sum := uint64(0)
	i := uint64(0)
	for ; float32(sum)/float32(totalHist) <= missRatio && i < uint64(len(m.Hist)); i++ {
		sum += m.Hist[i]
	}
	return float32(i) / float32(len(m.Hist))
}

func (m *mrc) getMissRatioFromNormalizedAvailableMemory(normalizedAvailableMmory float32) float32 {
	index := uint64(normalizedAvailableMmory * float32(m.Resolution-1))
	sum := uint64(0)
	for i := uint64(0); i <= index; i++ {
		sum += m.Hist[i]
	}
	return 1.0 - (float32(sum) / float32(m.getTotalSamplesInHistogram()))
}
