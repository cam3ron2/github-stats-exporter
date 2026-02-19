package githubapi

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const dayFormat = "2006-01-02"

// CopilotReportLinkResult is the typed result for Copilot report-link endpoints.
type CopilotReportLinkResult struct {
	Status         EndpointStatus
	URL            string
	DownloadLinks  []string
	ReportDay      time.Time
	ReportStartDay time.Time
	ReportEndDay   time.Time
	ExpiresAt      time.Time
	Metadata       CallMetadata
}

// CopilotReportStreamResult is the typed result for streaming Copilot report NDJSON data.
type CopilotReportStreamResult struct {
	Status        EndpointStatus
	RecordsParsed int
	ParseErrors   int
	Metadata      CallMetadata
}

// GetOrgCopilotOrganization1DayReportLink gets an org 1-day aggregate report link.
func (c *DataClient) GetOrgCopilotOrganization1DayReportLink(
	ctx context.Context,
	org string,
	day time.Time,
) (CopilotReportLinkResult, error) {
	return c.getCopilotReportLink(
		ctx,
		"orgs",
		org,
		"organization-1-day",
		day,
		true,
	)
}

// GetOrgCopilotOrganization28DayLatestReportLink gets an org 28-day latest aggregate report link.
func (c *DataClient) GetOrgCopilotOrganization28DayLatestReportLink(
	ctx context.Context,
	org string,
) (CopilotReportLinkResult, error) {
	return c.getCopilotReportLink(
		ctx,
		"orgs",
		org,
		"organization-28-day/latest",
		time.Time{},
		false,
	)
}

// GetOrgCopilotUsers1DayReportLink gets an org 1-day user report link.
func (c *DataClient) GetOrgCopilotUsers1DayReportLink(
	ctx context.Context,
	org string,
	day time.Time,
) (CopilotReportLinkResult, error) {
	return c.getCopilotReportLink(ctx, "orgs", org, "users-1-day", day, true)
}

// GetOrgCopilotUsers28DayLatestReportLink gets an org 28-day latest user report link.
func (c *DataClient) GetOrgCopilotUsers28DayLatestReportLink(
	ctx context.Context,
	org string,
) (CopilotReportLinkResult, error) {
	return c.getCopilotReportLink(ctx, "orgs", org, "users-28-day/latest", time.Time{}, false)
}

// GetEnterpriseCopilotEnterprise1DayReportLink gets an enterprise 1-day aggregate report link.
func (c *DataClient) GetEnterpriseCopilotEnterprise1DayReportLink(
	ctx context.Context,
	enterprise string,
	day time.Time,
) (CopilotReportLinkResult, error) {
	return c.getCopilotReportLink(
		ctx,
		"enterprises",
		enterprise,
		"enterprise-1-day",
		day,
		true,
	)
}

// GetEnterpriseCopilotEnterprise28DayLatestReportLink gets an enterprise 28-day latest aggregate report link.
func (c *DataClient) GetEnterpriseCopilotEnterprise28DayLatestReportLink(
	ctx context.Context,
	enterprise string,
) (CopilotReportLinkResult, error) {
	return c.getCopilotReportLink(
		ctx,
		"enterprises",
		enterprise,
		"enterprise-28-day/latest",
		time.Time{},
		false,
	)
}

// GetEnterpriseCopilotUsers1DayReportLink gets an enterprise 1-day user report link.
func (c *DataClient) GetEnterpriseCopilotUsers1DayReportLink(
	ctx context.Context,
	enterprise string,
	day time.Time,
) (CopilotReportLinkResult, error) {
	return c.getCopilotReportLink(
		ctx,
		"enterprises",
		enterprise,
		"users-1-day",
		day,
		true,
	)
}

// GetEnterpriseCopilotUsers28DayLatestReportLink gets an enterprise 28-day latest user report link.
func (c *DataClient) GetEnterpriseCopilotUsers28DayLatestReportLink(
	ctx context.Context,
	enterprise string,
) (CopilotReportLinkResult, error) {
	return c.getCopilotReportLink(
		ctx,
		"enterprises",
		enterprise,
		"users-28-day/latest",
		time.Time{},
		false,
	)
}

func (c *DataClient) getCopilotReportLink(
	ctx context.Context,
	scopePath string,
	owner string,
	reportPath string,
	day time.Time,
	requireDay bool,
) (CopilotReportLinkResult, error) {
	trimmedOwner := strings.TrimSpace(owner)
	if trimmedOwner == "" {
		return CopilotReportLinkResult{}, fmt.Errorf("owner is required")
	}
	if requireDay && day.IsZero() {
		return CopilotReportLinkResult{}, fmt.Errorf("day is required")
	}

	reqURL := c.cloneBaseURL()
	reqURL.Path = joinURLPath(
		reqURL.Path,
		scopePath,
		url.PathEscape(trimmedOwner),
		"copilot",
		"metrics",
		"reports",
		reportPath,
	)
	if requireDay {
		query := reqURL.Query()
		query.Set("date", day.UTC().Format(dayFormat))
		reqURL.RawQuery = query.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL.String(), nil)
	if err != nil {
		return CopilotReportLinkResult{}, fmt.Errorf("build copilot report link request: %w", err)
	}

	resp, metadata, err := c.requestClient.Do(req)
	if err != nil {
		return CopilotReportLinkResult{}, fmt.Errorf("copilot report link request failed: %w", err)
	}
	if resp == nil {
		return CopilotReportLinkResult{}, fmt.Errorf("copilot report link request failed: nil response")
	}

	status := endpointStatusFromHTTP(resp.StatusCode)
	result := CopilotReportLinkResult{
		Status:   status,
		Metadata: metadata,
	}
	if status != EndpointStatusOK {
		_ = resp.Body.Close()
		return result, nil
	}

	var payload copilotReportLinkPayload
	if err := decodeJSONAndClose(resp, &payload); err != nil {
		return CopilotReportLinkResult{}, fmt.Errorf("decode copilot report link response: %w", err)
	}

	downloadLinks := make([]string, 0, len(payload.DownloadLinks))
	for _, candidate := range payload.DownloadLinks {
		trimmed := strings.TrimSpace(candidate)
		if trimmed != "" {
			downloadLinks = append(downloadLinks, trimmed)
		}
	}
	if len(downloadLinks) == 0 {
		reportURL := strings.TrimSpace(payload.URL)
		if reportURL == "" {
			reportURL = strings.TrimSpace(payload.DownloadURL)
		}
		if reportURL != "" {
			downloadLinks = append(downloadLinks, reportURL)
		}
	}
	if len(downloadLinks) == 0 {
		return CopilotReportLinkResult{}, fmt.Errorf("copilot report link response missing url")
	}
	result.DownloadLinks = downloadLinks
	result.URL = downloadLinks[0]
	result.ReportDay = parseDay(payload.ReportDay)
	result.ReportStartDay = parseDay(payload.ReportStartDay)
	result.ReportEndDay = parseDay(payload.ReportEndDay)
	result.ExpiresAt = parseRFC3339(payload.ExpiresAt)
	return result, nil
}

// StreamCopilotReportNDJSON downloads and line-parses a Copilot report payload.
func (c *DataClient) StreamCopilotReportNDJSON(
	ctx context.Context,
	signedReportURL string,
	handler func(record map[string]any) error,
) (CopilotReportStreamResult, error) {
	trimmedURL := strings.TrimSpace(signedReportURL)
	if trimmedURL == "" {
		return CopilotReportStreamResult{}, fmt.Errorf("signed report url is required")
	}
	parsed, err := url.Parse(trimmedURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return CopilotReportStreamResult{}, fmt.Errorf("signed report url is invalid")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return CopilotReportStreamResult{}, fmt.Errorf("build copilot report download request: %w", err)
	}

	resp, metadata, err := c.requestClient.Do(req)
	if err != nil {
		return CopilotReportStreamResult{}, fmt.Errorf("copilot report download request failed: %w", err)
	}
	if resp == nil {
		return CopilotReportStreamResult{}, fmt.Errorf("copilot report download request failed: nil response")
	}

	status := endpointStatusFromHTTP(resp.StatusCode)
	result := CopilotReportStreamResult{
		Status:   status,
		Metadata: metadata,
	}
	if status != EndpointStatusOK {
		_ = resp.Body.Close()
		return result, nil
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			result.ParseErrors++
			continue
		}

		result.RecordsParsed++
		if handler != nil {
			if err := handler(record); err != nil {
				if closeErr := resp.Body.Close(); closeErr != nil {
					return CopilotReportStreamResult{}, fmt.Errorf(
						"handle copilot report line %d: %w (close body: %v)",
						lineNumber,
						err,
						closeErr,
					)
				}
				return CopilotReportStreamResult{}, fmt.Errorf(
					"handle copilot report line %d: %w",
					lineNumber,
					err,
				)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		if closeErr := resp.Body.Close(); closeErr != nil {
			return CopilotReportStreamResult{}, fmt.Errorf(
				"scan copilot report stream: %w (close body: %v)",
				err,
				closeErr,
			)
		}
		return CopilotReportStreamResult{}, fmt.Errorf("scan copilot report stream: %w", err)
	}
	if err := resp.Body.Close(); err != nil {
		return CopilotReportStreamResult{}, fmt.Errorf("close copilot report stream: %w", err)
	}
	return result, nil
}

type copilotReportLinkPayload struct {
	URL            string   `json:"url"`
	DownloadURL    string   `json:"download_url"`
	DownloadLinks  []string `json:"download_links"`
	ReportDay      string   `json:"report_day"`
	ReportStartDay string   `json:"report_start_day"`
	ReportEndDay   string   `json:"report_end_day"`
	ExpiresAt      string   `json:"expires_at"`
}

func parseDay(raw string) time.Time {
	parsed, err := time.Parse(dayFormat, raw)
	if err != nil {
		return time.Time{}
	}
	return parsed.UTC()
}
