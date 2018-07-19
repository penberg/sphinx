#!/usr/bin/env python3


def get_irq_smp_affinity(irq):
    with open("/proc/irq/%s/smp_affinity" % (irq)) as f:
        return f.read().strip()


def list_irqs():
    with open('/proc/interrupts') as f:
        lines = f.readlines()
        lines.pop(0)
        for raw_line in lines:
            line = raw_line.split()
            device = line[-1].strip()
            irq = line[0].strip()[0:-1]
            if not irq.isdigit():
                continue
            smp_affinity = get_irq_smp_affinity(irq)
            print("%s = %s %s" % (device, irq, smp_affinity))

if __name__ == '__main__':
    list_irqs()
