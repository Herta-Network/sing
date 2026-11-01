package domain_test

import (
	"bytes"
	"testing"

	"github.com/metacubex/sing/common/domain"

	"github.com/stretchr/testify/require"
)

func TestMatcher(t *testing.T) {
	t.Parallel()
	testDomain := []string{"example.com", "example.org"}
	testDomainSuffix := []string{".com.cn", ".org.cn", "sagernet.org"}
	matcher := domain.NewMatcher(testDomain, testDomainSuffix, false)
	require.NotNil(t, matcher)
	require.True(t, matcher.Match("example.com"))
	require.True(t, matcher.Match("example.org"))
	require.False(t, matcher.Match("example.cn"))
	require.True(t, matcher.Match("example.com.cn"))
	require.True(t, matcher.Match("example.org.cn"))
	require.False(t, matcher.Match("com.cn"))
	require.False(t, matcher.Match("org.cn"))
	require.True(t, matcher.Match("sagernet.org"))
	require.True(t, matcher.Match("sing-box.sagernet.org"))
	dDomain, dDomainSuffix := matcher.Dump()
	require.Equal(t, testDomain, dDomain)
	require.Equal(t, testDomainSuffix, dDomainSuffix)
}

func TestMatcherLegacy(t *testing.T) {
	t.Parallel()
	testDomain := []string{"example.com", "example.org"}
	testDomainSuffix := []string{".com.cn", ".org.cn", "sagernet.org"}
	matcher := domain.NewMatcher(testDomain, testDomainSuffix, true)
	require.NotNil(t, matcher)
	require.True(t, matcher.Match("example.com"))
	require.True(t, matcher.Match("example.org"))
	require.False(t, matcher.Match("example.cn"))
	require.True(t, matcher.Match("example.com.cn"))
	require.True(t, matcher.Match("example.org.cn"))
	require.False(t, matcher.Match("com.cn"))
	require.False(t, matcher.Match("org.cn"))
	require.True(t, matcher.Match("sagernet.org"))
	require.True(t, matcher.Match("sing-box.sagernet.org"))
	dDomain, dDomainSuffix := matcher.Dump()
	require.Equal(t, testDomain, dDomain)
	require.Equal(t, testDomainSuffix, dDomainSuffix)
}

func TestMatcherCollapsesDuplicateKeys(t *testing.T) {
	t.Parallel()
	// The seen map deduplicates the input strings, but distinct inputs can still
	// produce the same reversed key. A literal prefix label collides with a
	// dot-prefixed suffix, and invalid UTF-8 collapses to the same replacement
	// bytes. Duplicate keys used to make the succinct set builder index past the
	// end of the shorter key.
	for _, testCase := range []struct {
		name         string
		domain       []string
		domainSuffix []string
		match        string
	}{
		{"prefix label", []string{"\r.example.com"}, []string{".example.com"}, "www.example.com"},
		{"invalid utf-8", []string{"x.\xfe", "x.\xff"}, nil, ""},
	} {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			var matcher *domain.Matcher
			require.NotPanics(t, func() {
				matcher = domain.NewMatcher(testCase.domain, testCase.domainSuffix, false)
			})
			if testCase.match != "" {
				require.True(t, matcher.Match(testCase.match))
			}
			var encoded bytes.Buffer
			require.NoError(t, matcher.Write(&encoded))
			_, err := domain.ReadMatcher(&encoded)
			require.NoError(t, err)
		})
	}
}
