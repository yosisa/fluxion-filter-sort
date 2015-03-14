package main

import (
	"sort"
	"sync"
	"time"

	"github.com/yosisa/fluxion/buffer"
	"github.com/yosisa/fluxion/message"
	"github.com/yosisa/fluxion/plugin"
)

type event struct {
	t  time.Time
	ev *message.Event
}

func wrap(ev *message.Event) *event {
	return &event{t: time.Now(), ev: ev}
}

type Config struct {
	Tag      string
	Duration buffer.Duration
}

type FilterSort struct {
	env      *plugin.Env
	conf     Config
	events   []*event
	notify   chan struct{}
	stop     chan struct{}
	stopOnce sync.Once
	m        sync.Mutex
}

func (p *FilterSort) Init(env *plugin.Env) (err error) {
	p.env = env
	if err = env.ReadConfig(&p.conf); err != nil {
		return
	}
	p.notify = make(chan struct{}, 1)
	p.stop = make(chan struct{})
	return
}

func (p *FilterSort) Start() error {
	go p.asyncEmitter()
	return nil
}

func (p *FilterSort) Filter(ev *message.Event) (*message.Event, error) {
	p.m.Lock()
	p.events = append(p.events, wrap(ev))
	sort.Sort(ByTime(p.events))
	p.m.Unlock()
	select {
	case p.notify <- struct{}{}:
	default:
	}
	return nil, nil
}

func (p *FilterSort) asyncEmitter() {
	var timeout <-chan time.Time
	for {
		select {
		case <-p.notify:
		case <-timeout:
		case <-p.stop:
			p.flush()
			return
		}
		p.m.Lock()
		var emitted int
		for _, ev := range p.events {
			wait := time.Duration(p.conf.Duration) - time.Now().Sub(ev.t)
			if wait > 0 {
				timeout = time.After(wait)
				break
			}
			ev.ev.Tag = p.conf.Tag
			p.env.Emit(ev.ev)
			emitted++
		}
		n := copy(p.events, p.events[emitted:])
		p.events = p.events[:n]
		p.m.Unlock()
	}
}

func (p *FilterSort) flush() {
	p.m.Lock()
	defer p.m.Unlock()
	sort.Sort(ByTime(p.events))
	for _, ev := range p.events {
		ev.ev.Tag = p.conf.Tag
		p.env.Emit(ev.ev)
	}
}

func (p *FilterSort) Close() error {
	p.stopOnce.Do(func() { close(p.stop) })
	return nil
}

type ByTime []*event

func (s ByTime) Len() int {
	return len(s)
}

func (s ByTime) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s ByTime) Less(i, j int) bool {
	return s[i].t.Before(s[j].t)
}

func main() {
	plugin.New("filter-sort", func() plugin.Plugin { return &FilterSort{} }).Run()
}
