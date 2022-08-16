package client

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes/any"
	p4_config_v1 "github.com/p4lang/p4runtime/go/p4/config/v1"
	p4_v1 "github.com/p4lang/p4runtime/go/p4/v1"

	"github.com/RainyBow/p4runtime-go-client/pkg/util/conversion"
)

const (
	tableWildcardReadChSize = 100
)

func ToCanonicalIf(v []byte, cond bool) []byte {
	if cond {
		return conversion.ToCanonicalBytestring(v)
	} else {
		return v
	}
}

type MatchInterface interface {
	get(ID uint32, canonical bool) *p4_v1.FieldMatch
}

type ExactMatch struct {
	Value []byte
}

func (m *ExactMatch) get(ID uint32, canonical bool) *p4_v1.FieldMatch {
	exact := &p4_v1.FieldMatch_Exact{
		Value: ToCanonicalIf(m.Value, canonical),
	}
	mf := &p4_v1.FieldMatch{
		FieldId:        ID,
		FieldMatchType: &p4_v1.FieldMatch_Exact_{Exact: exact},
	}
	return mf
}

type LpmMatch struct {
	Value []byte
	PLen  int32
}

func (m *LpmMatch) get(ID uint32, canonical bool) *p4_v1.FieldMatch {
	lpm := &p4_v1.FieldMatch_LPM{
		Value:     m.Value,
		PrefixLen: m.PLen,
	}

	// P4Runtime has strict rules regarding ternary matches: in the case of
	// LPM, trailing bits in the value (after prefix) must be set to 0.
	firstByteMasked := int(m.PLen / 8)
	if firstByteMasked != len(lpm.Value) {
		i := firstByteMasked
		r := m.PLen % 8
		lpm.Value[i] = lpm.Value[i] & (0xff << (8 - r))
		for i = i + 1; i < len(lpm.Value); i++ {
			lpm.Value[i] = 0
		}
	}

	lpm.Value = ToCanonicalIf(lpm.Value, canonical)

	mf := &p4_v1.FieldMatch{
		FieldId:        ID,
		FieldMatchType: &p4_v1.FieldMatch_Lpm{Lpm: lpm},
	}
	return mf
}

type TernaryMatch struct {
	Value []byte
	Mask  []byte
}

func (m *TernaryMatch) get(ID uint32, canonical bool) *p4_v1.FieldMatch {
	ternary := &p4_v1.FieldMatch_Ternary{
		Value: m.Value,
		Mask:  m.Mask,
	}

	// P4Runtime has strict rules regarding ternary matches: masked off bits
	// must be set to 0 in the value.
	offset := len(ternary.Mask) - len(ternary.Value)
	if offset < 0 {
		ternary.Value = ternary.Value[-offset:]
		offset = 0
	}

	for i, b := range ternary.Mask[offset:] {
		ternary.Value[i] = ternary.Value[i] & b
	}

	ternary.Value = ToCanonicalIf(ternary.Value, canonical)
	ternary.Mask = ToCanonicalIf(ternary.Mask, canonical)

	mf := &p4_v1.FieldMatch{
		FieldId:        ID,
		FieldMatchType: &p4_v1.FieldMatch_Ternary_{Ternary: ternary},
	}
	return mf
}

type RangeMatch struct {
	Low  []byte
	High []byte
}

func (m *RangeMatch) get(ID uint32, canonical bool) *p4_v1.FieldMatch {
	fmRange := &p4_v1.FieldMatch_Range{
		Low:  ToCanonicalIf(m.Low, canonical),
		High: ToCanonicalIf(m.High, canonical),
	}

	mf := &p4_v1.FieldMatch{
		FieldId:        ID,
		FieldMatchType: &p4_v1.FieldMatch_Range_{Range: fmRange},
	}
	return mf
}

type OptionalMatch struct {
	Value []byte
}

func (m *OptionalMatch) get(ID uint32, canonical bool) *p4_v1.FieldMatch {
	optional := &p4_v1.FieldMatch_Optional{
		Value: ToCanonicalIf(m.Value, canonical),
	}

	mf := &p4_v1.FieldMatch{
		FieldId:        ID,
		FieldMatchType: &p4_v1.FieldMatch_Optional_{Optional: optional},
	}
	return mf
}

type TableEntryOptions struct {
	IdleTimeout time.Duration
	Priority    int32
}

func (c *Client) newAction(action string, params [][]byte) *p4_v1.Action {
	actionID := c.actionId(action)
	directAction := &p4_v1.Action{
		ActionId: actionID,
	}

	for idx, p := range params {
		param := &p4_v1.Action_Param{
			ParamId: uint32(idx + 1),
			Value:   p,
		}
		directAction.Params = append(directAction.Params, param)
	}

	return directAction
}

func (c *Client) NewTableActionDirect(
	action string,
	params [][]byte,
) *p4_v1.TableAction {
	return &p4_v1.TableAction{
		Type: &p4_v1.TableAction_Action{Action: c.newAction(action, params)},
	}
}

type ActionProfileActionSet struct {
	client *Client
	action *p4_v1.TableAction
}

func (c *Client) NewActionProfileActionSet() *ActionProfileActionSet {
	return &ActionProfileActionSet{
		client: c,
		action: &p4_v1.TableAction{
			Type: &p4_v1.TableAction_ActionProfileActionSet{
				ActionProfileActionSet: &p4_v1.ActionProfileActionSet{},
			},
		},
	}
}

func (s *ActionProfileActionSet) AddAction(
	action string,
	params [][]byte,
	weight int32,
	port Port,
) *ActionProfileActionSet {
	actionSet := s.action.GetActionProfileActionSet()
	actionSet.ActionProfileActions = append(
		actionSet.ActionProfileActions,
		&p4_v1.ActionProfileAction{
			Action: s.client.newAction(action, params),
			Weight: weight,
			WatchKind: &p4_v1.ActionProfileAction_WatchPort{
				WatchPort: port.AsBytes(),
			},
		},
	)
	return s
}

func (s *ActionProfileActionSet) TableAction() *p4_v1.TableAction {
	return s.action
}

// for default entries: to set use nil for mfs, to unset use nil for mfs and nil
// for action
func (c *Client) NewTableEntry(
	table string,
	mfs map[string]MatchInterface,
	action *p4_v1.TableAction,
	options *TableEntryOptions,
) *p4_v1.TableEntry {
	tableID := c.tableId(table)

	entry := &p4_v1.TableEntry{
		TableId: tableID,
		//nolint:staticcheck // SA5011 if mfs==nil then for loop is not executed by default
		IsDefaultAction: (mfs == nil),
		Action:          action,
	}

	//nolint:staticcheck // SA5011 if mfs==nil then for loop is not executed by default
	//lint:ignore SA5011 This line added for support golint version of VSC
	for name, mf := range mfs {
		fieldID := c.matchFieldId(table, name)
		entry.Match = append(entry.Match, mf.get(fieldID, c.CanonicalBytestrings))
	}

	if options != nil {
		entry.IdleTimeoutNs = options.IdleTimeout.Nanoseconds()
		entry.Priority = options.Priority
	}

	return entry
}

func (c *Client) ReadTableEntry(ctx context.Context, table string, mfs []MatchInterface) (*p4_v1.TableEntry, error) {
	tableID := c.tableId(table)

	entry := &p4_v1.TableEntry{
		TableId: tableID,
	}

	for idx, mf := range mfs {
		entry.Match = append(entry.Match, mf.get(uint32(idx+1), c.CanonicalBytestrings))
	}

	entity := &p4_v1.Entity{
		Entity: &p4_v1.Entity_TableEntry{TableEntry: entry},
	}

	readEntity, err := c.ReadEntitySingle(ctx, entity)
	if err != nil {
		return nil, fmt.Errorf("error when reading table entry: %v", err)
	}

	readEntry := readEntity.GetTableEntry()
	if readEntry == nil {
		return nil, fmt.Errorf("server returned an entity but it is not a table entry! ")
	}

	return readEntry, nil
}

func (c *Client) ReadTableEntryWildcard(ctx context.Context, table string) ([]*p4_v1.TableEntry, error) {
	tableID := c.tableId(table)

	entry := &p4_v1.TableEntry{
		TableId: tableID,
	}

	out := make([]*p4_v1.TableEntry, 0)
	readEntityCh := make(chan *p4_v1.Entity, tableWildcardReadChSize)

	var wg sync.WaitGroup
	var err error
	wg.Add(1)

	go func() {
		defer wg.Done()
		for readEntity := range readEntityCh {
			readEntry := readEntity.GetTableEntry()
			if readEntry != nil {
				out = append(out, readEntry)
			} else if err == nil {
				// only set the error if this is the first error we encounter
				// dp not stop reading from the channel, as doing so would cause
				// ReadEntityWildcard to block indefinitely
				err = fmt.Errorf("server returned an entity which is not a table entry!")
			}
		}
	}()

	if err := c.ReadEntityWildcard(ctx, &p4_v1.Entity{
		Entity: &p4_v1.Entity_TableEntry{TableEntry: entry},
	}, readEntityCh); err != nil {
		return nil, fmt.Errorf("error when reading table entries: %v", err)
	}

	wg.Wait()
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Client) InsertTableEntry(ctx context.Context, entry *p4_v1.TableEntry) error {
	update := &p4_v1.Update{
		Type: p4_v1.Update_INSERT,
		Entity: &p4_v1.Entity{
			Entity: &p4_v1.Entity_TableEntry{TableEntry: entry},
		},
	}

	return c.WriteUpdate(ctx, update)
}

func (c *Client) ModifyTableEntry(ctx context.Context, entry *p4_v1.TableEntry) error {
	update := &p4_v1.Update{
		Type: p4_v1.Update_MODIFY,
		Entity: &p4_v1.Entity{
			Entity: &p4_v1.Entity_TableEntry{TableEntry: entry},
		},
	}

	return c.WriteUpdate(ctx, update)
}

func (c *Client) DeleteTableEntry(ctx context.Context, entry *p4_v1.TableEntry) error {
	update := &p4_v1.Update{
		Type: p4_v1.Update_DELETE,
		Entity: &p4_v1.Entity{
			Entity: &p4_v1.Entity_TableEntry{TableEntry: entry},
		},
	}

	return c.WriteUpdate(ctx, update)
}

func formartByts2String(byts []byte) string {
	switch len(byts) {
	case 4:
		// ipv4
		char_list := []string{}
		for _, b := range byts {
			char_list = append(char_list, fmt.Sprintf("%d", b))
		}
		return strings.Join(char_list, ".")
	case 6:
		char_list := []string{}
		for _, b := range byts {
			char_list = append(char_list, fmt.Sprintf("%02x", b))
		}
		return strings.Join(char_list, ":")
	case 16:
		char_list := []string{}
		for _, b := range byts {
			char_list = append(char_list, fmt.Sprintf("%02x", b))
		}
		return net.ParseIP(strings.Join(char_list, ":")).To16().String()
	case 1:
		return fmt.Sprintf("%d", byts[0])
	default:
		var n int64
		for _, b := range byts {
			n = n<<8 + int64(b)
		}
		return fmt.Sprintf("%d", n)
	}
}

type TableEntry struct {
	Name     string        `json:"table_name"`
	Fields   []*MatchField `json:"fields"`
	Action   *Action       `json:"action"`
	Priority int32         `json:"priority"`
}

func (table_entry *TableEntry) String() string {
	field_list := []string{}
	for _, field := range table_entry.Fields {
		field_list = append(field_list, field.String())
	}
	return fmt.Sprintf("Table %s: %s => %s", table_entry.Name, strings.Join(field_list, ","), table_entry.Action.String())
}

type MatchType string

const (
	Exact    MatchType = "exact"
	Ternary  MatchType = "ternary"
	Lpm      MatchType = "lpm"
	Range    MatchType = "range"
	Optional MatchType = "optional"
	Other    MatchType = "other"
)

type MatchField struct {
	Name      string    `json:"field_name"`           // field name
	Type      MatchType `json:"match_type"`           // match type
	Value     []byte    `json:"value,omitempty"`      // match value  for
	Mask      []byte    `json:"mask,omitempty"`       // just for type Ternary
	PrefixLen int32     `json:"prefix_len,omitempty"` // just for type Lpm
	Low       []byte    `json:"low,omitempty"`        // just for type Range
	High      []byte    `json:"high,omitempty"`       // just for type Range

}

func (mf *MatchField) String() string {
	s := fmt.Sprintf("%s(%s)=", mf.Name, mf.Type)
	switch mf.Type {
	case Exact, Optional, Other:
		s += formartByts2String(mf.Value)
	case Lpm:
		s += fmt.Sprintf("[%s,%d]", formartByts2String(mf.Value), mf.PrefixLen)
	case Ternary:
		s += fmt.Sprintf("[%s &&& %s]", formartByts2String(mf.Value), formartByts2String(mf.Mask))
	case Range:
		s += fmt.Sprintf("[%s-%s]", formartByts2String(mf.Low), formartByts2String(mf.High))
	}
	return s
}

type Action struct {
	Name   string         `json:"action_name"`
	Params []*ActionParam `json:"table_params"`
}

func (action *Action) String() string {

	s := action.Name
	if len(action.Params) > 0 {
		param_list := []string{}
		for _, param := range action.Params {
			param_list = append(param_list, param.String())
		}
		s = s + fmt.Sprintf("(%s)", strings.Join(param_list, ","))
	}
	return s
}

type ActionParam struct {
	Name  string `json:"param_name"`
	Value []byte `json:"param_value"`
}

func (ap *ActionParam) String() string {
	return fmt.Sprintf("%s=%v", ap.Name, formartByts2String(ap.Value))
}

// TableEntryEncode convert TableEntry to p4_v1.TableEntry
func (c *Client) TableEntryEncode(table_entry *TableEntry) (p4_table_entry *p4_v1.TableEntry, err error) {
	p4_table_entry = &p4_v1.TableEntry{}
	p4_table_entry.TableId = c.tableId(table_entry.Name)
	p4_table_entry.Priority = table_entry.Priority
	if p4_table_entry.TableId == invalidID {
		err = fmt.Errorf("unknown table name:%s", table_entry.Name)
		return
	}
	// do match field
	p4_table_entry.Match = []*p4_v1.FieldMatch{}
	for _, field := range table_entry.Fields {
		field_id := c.matchFieldId(table_entry.Name, field.Name)
		if field_id == invalidID {
			err = fmt.Errorf("unknown field name:%s in table %s", field.Name, table_entry.Name)
			return
		}
		var match_field *p4_v1.FieldMatch
		switch field.Type {
		case Exact:
			match_field = &p4_v1.FieldMatch{
				FieldId: field_id,
				FieldMatchType: &p4_v1.FieldMatch_Exact_{
					Exact: &p4_v1.FieldMatch_Exact{Value: field.Value},
				},
			}
		case Lpm:
			match_field = &p4_v1.FieldMatch{
				FieldId: field_id,
				FieldMatchType: &p4_v1.FieldMatch_Lpm{
					Lpm: &p4_v1.FieldMatch_LPM{Value: field.Value, PrefixLen: field.PrefixLen},
				},
			}
		case Ternary:
			match_field = &p4_v1.FieldMatch{
				FieldId: field_id,
				FieldMatchType: &p4_v1.FieldMatch_Ternary_{
					Ternary: &p4_v1.FieldMatch_Ternary{Value: field.Value, Mask: field.Mask},
				},
			}
		case Range:
			match_field = &p4_v1.FieldMatch{
				FieldId: field_id,
				FieldMatchType: &p4_v1.FieldMatch_Range_{
					Range: &p4_v1.FieldMatch_Range{Low: field.Low, High: field.High},
				},
			}
		case Optional:
			match_field = &p4_v1.FieldMatch{
				FieldId: field_id,
				FieldMatchType: &p4_v1.FieldMatch_Optional_{
					Optional: &p4_v1.FieldMatch_Optional{Value: field.Value},
				},
			}
		case Other:
			match_field = &p4_v1.FieldMatch{
				FieldId: field_id,
				FieldMatchType: &p4_v1.FieldMatch_Other{
					Other: &any.Any{Value: field.Value},
				},
			}
		}
		p4_table_entry.Match = append(p4_table_entry.Match, match_field)
	}
	// do action
	action := &p4_v1.Action{ActionId: c.actionId(table_entry.Action.Name)}
	if action.ActionId == invalidID {
		err = fmt.Errorf("unknown action name:%s", table_entry.Action.Name)
		return
	}
	action.Params = []*p4_v1.Action_Param{}
	for _, param := range table_entry.Action.Params {
		action_param := &p4_v1.Action_Param{
			ParamId: c.actionParamId(table_entry.Action.Name, param.Name),
			Value:   param.Value,
		}
		if action_param.ParamId == invalidID {
			err = fmt.Errorf("unknown param name:%s in action %s", param.Name, table_entry.Action.Name)
			return
		}
		action.Params = append(action.Params, action_param)
	}
	p4_table_entry.Action = &p4_v1.TableAction{
		Type: &p4_v1.TableAction_Action{Action: action},
	}
	return
}

// TableEntryDecode   p4_v1.TableEntry to  TableEntry
func (c *Client) TableEntryDecode(p4_table_entry *p4_v1.TableEntry) (table_entry *TableEntry, err error) {
	table_entry = &TableEntry{}
	table := c.findTableById(p4_table_entry.TableId)
	if table == nil {
		err = fmt.Errorf("can not find table(id=%d) in p4info", p4_table_entry.TableId)
		return
	}
	table_entry.Name = table.Preamble.Name
	table_entry.Fields = []*MatchField{}
	for _, field := range p4_table_entry.Match {
		field_match := c.findFieldInTable(table, field.FieldId)
		if table == nil {
			err = fmt.Errorf("can not find match field(id=%d) on table %s in p4info", field.FieldId, table_entry.Name)
			return
		}
		match_field := &MatchField{Name: field_match.Name}
		switch field_match.GetMatchType() {
		case p4_config_v1.MatchField_EXACT:
			match_field.Type = Exact
			match_field.Value = field.GetExact().GetValue()
		case p4_config_v1.MatchField_LPM:
			match_field.Type = Lpm
			match_field.Value = field.GetLpm().GetValue()
			match_field.PrefixLen = field.GetLpm().GetPrefixLen()
		case p4_config_v1.MatchField_TERNARY:
			match_field.Type = Ternary
			match_field.Value = field.GetTernary().GetValue()
			match_field.Mask = field.GetTernary().GetMask()
		case p4_config_v1.MatchField_RANGE:
			match_field.Type = Range
			match_field.Low = field.GetRange().GetLow()
			match_field.High = field.GetRange().GetHigh()
		case p4_config_v1.MatchField_OPTIONAL:
			match_field.Type = Optional
			match_field.Value = field.GetOptional().GetValue()
		default:
			match_field.Type = Other
			match_field.Value = field.GetOther().GetValue()
		}
		table_entry.Fields = append(table_entry.Fields, match_field)
	}
	// do action
	p4_action := p4_table_entry.Action.GetAction()
	action := c.getActionById(p4_action.ActionId)
	if action == nil {
		err = fmt.Errorf("can not find action(id=%d) in p4info", p4_action.ActionId)
		return
	}
	table_entry.Action = &Action{Name: action.Preamble.Name, Params: []*ActionParam{}}
	for _, param := range p4_action.Params {
		action_param := &ActionParam{
			Name:  c.getActionParamName(action, param.ParamId),
			Value: param.Value,
		}
		if action_param.Name == unknownName {
			err = fmt.Errorf("can not find param(id=%d) in action %s", param.ParamId, table_entry.Action.Name)
			return
		}
		table_entry.Action.Params = append(table_entry.Action.Params, action_param)
	}

	return
}
